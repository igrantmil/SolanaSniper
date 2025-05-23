"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Bot = void 0;
const web3_js_1 = require("@solana/web3.js");
const spl_token_1 = require("@solana/spl-token");
const raydium_sdk_1 = require("@raydium-io/raydium-sdk");
const cache_1 = require("./cache");
const filters_1 = require("./filters");
const helpers_1 = require("./helpers");
const async_mutex_1 = require("async-mutex");
const bn_js_1 = __importDefault(require("bn.js"));
const warp_transaction_executor_1 = require("./transactions/warp-transaction-executor");
const jito_rpc_transaction_executor_1 = require("./transactions/jito-rpc-transaction-executor");
class Bot {
    constructor(connection, marketStorage, poolStorage, txExecutor, config) {
        this.connection = connection;
        this.marketStorage = marketStorage;
        this.poolStorage = poolStorage;
        this.txExecutor = txExecutor;
        this.config = config;
        this.sellExecutionCount = 0;
        this.stopLoss = new Map();
        this.isWarp = false;
        this.isJito = false;
        this.isWarp = txExecutor instanceof warp_transaction_executor_1.WarpTransactionExecutor;
        this.isJito = txExecutor instanceof jito_rpc_transaction_executor_1.JitoTransactionExecutor;
        this.semaphore = new async_mutex_1.Semaphore(config.maxTokensAtTheTime);
        if (this.config.useSnipeList) {
            this.snipeListCache = new cache_1.SnipeListCache();
            this.snipeListCache.init();
        }
    }
    validate() {
        return __awaiter(this, void 0, void 0, function* () {
            try {
                yield (0, spl_token_1.getAccount)(this.connection, this.config.quoteAta, this.connection.commitment);
            }
            catch (error) {
                helpers_1.logger.error(`${this.config.quoteToken.symbol} token account not found in wallet: ${this.config.wallet.publicKey.toString()}`);
                return false;
            }
            return true;
        });
    }
    buy(accountId, poolState) {
        var _a;
        return __awaiter(this, void 0, void 0, function* () {
            helpers_1.logger.trace({ mint: poolState.baseMint }, `Processing new pool...`);
            if (this.config.useSnipeList && !((_a = this.snipeListCache) === null || _a === void 0 ? void 0 : _a.isInList(poolState.baseMint.toString()))) {
                helpers_1.logger.debug({ mint: poolState.baseMint.toString() }, `Skipping buy because token is not in a snipe list`);
                return;
            }
            if (this.config.autoBuyDelay > 0) {
                helpers_1.logger.debug({ mint: poolState.baseMint }, `Waiting for ${this.config.autoBuyDelay} ms before buy`);
                yield (0, helpers_1.sleep)(this.config.autoBuyDelay);
            }
            const numberOfActionsBeingProcessed = this.config.maxTokensAtTheTime - this.semaphore.getValue() + this.sellExecutionCount;
            if (this.semaphore.isLocked() || numberOfActionsBeingProcessed >= this.config.maxTokensAtTheTime) {
                helpers_1.logger.debug({ mint: poolState.baseMint.toString() }, `Skipping buy because max tokens to process at the same time is ${this.config.maxTokensAtTheTime} and currently ${numberOfActionsBeingProcessed} tokens is being processed`);
                return;
            }
            yield this.semaphore.acquire();
            try {
                const [market, mintAta] = yield Promise.all([
                    this.marketStorage.get(poolState.marketId.toString()),
                    (0, spl_token_1.getAssociatedTokenAddress)(poolState.baseMint, this.config.wallet.publicKey),
                ]);
                const poolKeys = (0, helpers_1.createPoolKeys)(accountId, poolState, market);
                if (!this.config.useSnipeList) {
                    const match = yield this.filterMatch(poolKeys);
                    if (!match) {
                        helpers_1.logger.trace({ mint: poolKeys.baseMint.toString() }, `Skipping buy because pool doesn't match filters`);
                        return;
                    }
                }
                for (let i = 0; i < this.config.maxBuyRetries; i++) {
                    try {
                        helpers_1.logger.info({ mint: poolState.baseMint.toString() }, `Send buy transaction attempt: ${i + 1}/${this.config.maxBuyRetries}`);
                        const tokenOut = new raydium_sdk_1.Token(spl_token_1.TOKEN_PROGRAM_ID, poolKeys.baseMint, poolKeys.baseDecimals);
                        const result = yield this.swap(poolKeys, this.config.quoteAta, mintAta, this.config.quoteToken, tokenOut, this.config.quoteAmount, this.config.buySlippage, this.config.wallet, 'buy');
                        if (result.confirmed) {
                            helpers_1.logger.info({
                                mint: poolState.baseMint.toString(),
                                signature: result.signature,
                                url: `https://solscan.io/tx/${result.signature}?cluster=${helpers_1.NETWORK}`,
                            }, `Confirmed buy tx`);
                            break;
                        }
                        helpers_1.logger.info({
                            mint: poolState.baseMint.toString(),
                            signature: result.signature,
                            error: result.error,
                        }, `Error confirming buy tx`);
                    }
                    catch (error) {
                        helpers_1.logger.debug({ mint: poolState.baseMint.toString(), error }, `Error confirming buy transaction`);
                    }
                }
            }
            catch (error) {
                helpers_1.logger.error({ mint: poolState.baseMint.toString(), error }, `Failed to buy token`);
            }
            finally {
                this.semaphore.release();
            }
        });
    }
    sell(accountId, rawAccount) {
        return __awaiter(this, void 0, void 0, function* () {
            this.sellExecutionCount++;
            try {
                helpers_1.logger.trace({ mint: rawAccount.mint }, `Processing new token...`);
                const poolData = yield this.poolStorage.get(rawAccount.mint.toString());
                if (!poolData) {
                    helpers_1.logger.trace({ mint: rawAccount.mint.toString() }, `Token pool data is not found, can't sell`);
                    return;
                }
                const tokenIn = new raydium_sdk_1.Token(spl_token_1.TOKEN_PROGRAM_ID, poolData.state.baseMint, poolData.state.baseDecimal.toNumber());
                const tokenAmountIn = new raydium_sdk_1.TokenAmount(tokenIn, rawAccount.amount, true);
                if (tokenAmountIn.isZero()) {
                    helpers_1.logger.info({ mint: rawAccount.mint.toString() }, `Empty balance, can't sell`);
                    return;
                }
                if (this.config.autoSellDelay > 0) {
                    helpers_1.logger.debug({ mint: rawAccount.mint }, `Waiting for ${this.config.autoSellDelay} ms before sell`);
                    yield (0, helpers_1.sleep)(this.config.autoSellDelay);
                }
                const market = yield this.marketStorage.get(poolData.state.marketId.toString());
                const poolKeys = (0, helpers_1.createPoolKeys)(new web3_js_1.PublicKey(poolData.id), poolData.state, market);
                for (let i = 0; i < this.config.maxSellRetries; i++) {
                    try {
                        const shouldSell = yield this.waitForSellSignal(tokenAmountIn, poolKeys);
                        if (!shouldSell) {
                            return;
                        }
                        helpers_1.logger.info({ mint: rawAccount.mint }, `Send sell transaction attempt: ${i + 1}/${this.config.maxSellRetries}`);
                        const result = yield this.swap(poolKeys, accountId, this.config.quoteAta, tokenIn, this.config.quoteToken, tokenAmountIn, this.config.sellSlippage, this.config.wallet, 'sell');
                        if (result.confirmed) {
                            helpers_1.logger.info({
                                dex: `https://dexscreener.com/solana/${rawAccount.mint.toString()}?maker=${this.config.wallet.publicKey}`,
                                mint: rawAccount.mint.toString(),
                                signature: result.signature,
                                url: `https://solscan.io/tx/${result.signature}?cluster=${helpers_1.NETWORK}`,
                            }, `Confirmed sell tx`);
                            break;
                        }
                        helpers_1.logger.info({
                            mint: rawAccount.mint.toString(),
                            signature: result.signature,
                            error: result.error,
                        }, `Error confirming sell tx`);
                    }
                    catch (error) {
                        helpers_1.logger.debug({ mint: rawAccount.mint.toString(), error }, `Error confirming sell transaction`);
                    }
                }
            }
            catch (error) {
                helpers_1.logger.error({ mint: rawAccount.mint.toString(), error }, `Failed to sell token`);
            }
            finally {
                this.sellExecutionCount--;
            }
        });
    }
    // noinspection JSUnusedLocalSymbols
    swap(poolKeys, ataIn, ataOut, tokenIn, tokenOut, amountIn, slippage, wallet, direction) {
        return __awaiter(this, void 0, void 0, function* () {
            const slippagePercent = new raydium_sdk_1.Percent(slippage, 100);
            const poolInfo = yield raydium_sdk_1.Liquidity.fetchInfo({
                connection: this.connection,
                poolKeys,
            });
            const computedAmountOut = raydium_sdk_1.Liquidity.computeAmountOut({
                poolKeys,
                poolInfo,
                amountIn,
                currencyOut: tokenOut,
                slippage: slippagePercent,
            });
            const latestBlockhash = yield this.connection.getLatestBlockhash();
            const { innerTransaction } = raydium_sdk_1.Liquidity.makeSwapFixedInInstruction({
                poolKeys: poolKeys,
                userKeys: {
                    tokenAccountIn: ataIn,
                    tokenAccountOut: ataOut,
                    owner: wallet.publicKey,
                },
                amountIn: amountIn.raw,
                minAmountOut: computedAmountOut.minAmountOut.raw,
            }, poolKeys.version);
            const messageV0 = new web3_js_1.TransactionMessage({
                payerKey: wallet.publicKey,
                recentBlockhash: latestBlockhash.blockhash,
                instructions: [
                    ...(this.isWarp || this.isJito
                        ? []
                        : [
                            web3_js_1.ComputeBudgetProgram.setComputeUnitPrice({ microLamports: this.config.unitPrice }),
                            web3_js_1.ComputeBudgetProgram.setComputeUnitLimit({ units: this.config.unitLimit }),
                        ]),
                    ...(direction === 'buy'
                        ? [
                            (0, spl_token_1.createAssociatedTokenAccountIdempotentInstruction)(wallet.publicKey, ataOut, wallet.publicKey, tokenOut.mint),
                        ]
                        : []),
                    ...innerTransaction.instructions,
                    ...(direction === 'sell' ? [(0, spl_token_1.createCloseAccountInstruction)(ataIn, wallet.publicKey, wallet.publicKey)] : []),
                ],
            }).compileToV0Message();
            const transaction = new web3_js_1.VersionedTransaction(messageV0);
            transaction.sign([wallet, ...innerTransaction.signers]);
            return this.txExecutor.executeAndConfirm(transaction, wallet, latestBlockhash);
        });
    }
    filterMatch(poolKeys) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.config.filterCheckInterval === 0 || this.config.filterCheckDuration === 0) {
                return true;
            }
            const filters = new filters_1.PoolFilters(this.connection, {
                quoteToken: this.config.quoteToken,
                minPoolSize: this.config.minPoolSize,
                maxPoolSize: this.config.maxPoolSize,
            });
            const timesToCheck = this.config.filterCheckDuration / this.config.filterCheckInterval;
            let timesChecked = 0;
            let matchCount = 0;
            do {
                try {
                    const shouldBuy = yield filters.execute(poolKeys);
                    if (shouldBuy) {
                        matchCount++;
                        if (this.config.consecutiveMatchCount <= matchCount) {
                            helpers_1.logger.debug({ mint: poolKeys.baseMint.toString() }, `Filter match ${matchCount}/${this.config.consecutiveMatchCount}`);
                            return true;
                        }
                    }
                    else {
                        matchCount = 0;
                    }
                    yield (0, helpers_1.sleep)(this.config.filterCheckInterval);
                }
                finally {
                    timesChecked++;
                }
            } while (timesChecked < timesToCheck);
            return false;
        });
    }
    waitForSellSignal(amountIn, poolKeys) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.config.priceCheckDuration === 0 || this.config.priceCheckInterval === 0) {
                return true;
            }
            const timesToCheck = this.config.priceCheckDuration / this.config.priceCheckInterval;
            const profitFraction = this.config.quoteAmount.mul(this.config.takeProfit).numerator.div(new bn_js_1.default(100));
            const profitAmount = new raydium_sdk_1.TokenAmount(this.config.quoteToken, profitFraction, true);
            const takeProfit = this.config.quoteAmount.add(profitAmount);
            let stopLoss;
            if (!this.stopLoss.get(poolKeys.baseMint.toString())) {
                const lossFraction = this.config.quoteAmount.mul(this.config.stopLoss).numerator.div(new bn_js_1.default(100));
                const lossAmount = new raydium_sdk_1.TokenAmount(this.config.quoteToken, lossFraction, true);
                stopLoss = this.config.quoteAmount.subtract(lossAmount);
                this.stopLoss.set(poolKeys.baseMint.toString(), stopLoss);
            }
            else {
                stopLoss = this.stopLoss.get(poolKeys.baseMint.toString());
            }
            const slippage = new raydium_sdk_1.Percent(this.config.sellSlippage, 100);
            let timesChecked = 0;
            do {
                try {
                    const poolInfo = yield raydium_sdk_1.Liquidity.fetchInfo({
                        connection: this.connection,
                        poolKeys,
                    });
                    const amountOut = raydium_sdk_1.Liquidity.computeAmountOut({
                        poolKeys,
                        poolInfo,
                        amountIn: amountIn,
                        currencyOut: this.config.quoteToken,
                        slippage,
                    }).amountOut;
                    if (this.config.trailingStopLoss) {
                        const trailingLossFraction = amountOut.mul(this.config.stopLoss).numerator.div(new bn_js_1.default(100));
                        const trailingLossAmount = new raydium_sdk_1.TokenAmount(this.config.quoteToken, trailingLossFraction, true);
                        const trailingStopLoss = amountOut.subtract(trailingLossAmount);
                        if (trailingStopLoss.gt(stopLoss)) {
                            helpers_1.logger.trace({ mint: poolKeys.baseMint.toString() }, `Updating trailing stop loss from ${stopLoss.toFixed()} to ${trailingStopLoss.toFixed()}`);
                            this.stopLoss.set(poolKeys.baseMint.toString(), trailingStopLoss);
                            stopLoss = trailingStopLoss;
                        }
                    }
                    if (this.config.skipSellingIfLostMoreThan > 0) {
                        const stopSellingFraction = this.config.quoteAmount
                            .mul(this.config.skipSellingIfLostMoreThan)
                            .numerator.div(new bn_js_1.default(100));
                        const stopSellingAmount = new raydium_sdk_1.TokenAmount(this.config.quoteToken, stopSellingFraction, true);
                        if (amountOut.lt(stopSellingAmount)) {
                            helpers_1.logger.debug({ mint: poolKeys.baseMint.toString() }, `Token dropped more than ${this.config.skipSellingIfLostMoreThan}%, sell stopped. Initial: ${this.config.quoteAmount.toFixed()} | Current: ${amountOut.toFixed()}`);
                            this.stopLoss.delete(poolKeys.baseMint.toString());
                            return false;
                        }
                    }
                    helpers_1.logger.debug({ mint: poolKeys.baseMint.toString() }, `Take profit: ${takeProfit.toFixed()} | Stop loss: ${stopLoss.toFixed()} | Current: ${amountOut.toFixed()}`);
                    if (amountOut.lt(stopLoss)) {
                        this.stopLoss.delete(poolKeys.baseMint.toString());
                        break;
                    }
                    if (amountOut.gt(takeProfit)) {
                        this.stopLoss.delete(poolKeys.baseMint.toString());
                        break;
                    }
                    yield (0, helpers_1.sleep)(this.config.priceCheckInterval);
                }
                catch (e) {
                    helpers_1.logger.trace({ mint: poolKeys.baseMint.toString(), e }, `Failed to check token price`);
                }
                finally {
                    timesChecked++;
                }
            } while (timesChecked < timesToCheck);
            return true;
        });
    }
}
exports.Bot = Bot;
