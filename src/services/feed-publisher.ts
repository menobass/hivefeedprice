import {
  createWaxFoundation,
  type IHiveChainInterface,
  type IWaxBaseInterface,
  WitnessSetPropertiesOperation,
  type IWitnessSetPropertiesData,
  type TInternalAsset,
} from "@hiveio/wax";
import createBeekeeper, {
  type IBeekeeperUnlockedWallet,
  type IBeekeeperWallet,
  type IBeekeeperInstance,
  type IBeekeeperSession,
} from "@hiveio/beekeeper";
import { priceAggregator } from "@/services/price-aggregator";
import { loadConfig } from "@/config/config";
import { HiveChainWithFailover } from "@/utils/hive-chain-failover";

const BEEKEEPER = {
  WALLET_NAME: "feed-publisher" as const,
  WALLET_PASSWORD: process.env.BEEKEEPER_WALLET_PASSWORD || "auto-pass",
} as const;

interface HiveConfig {
  readonly witnessAccount: string;
  readonly privateKey: string;
  readonly rpcNodes?: readonly string[];
  readonly chainId?: string;
}

export class FeedPublisher {
  private readonly config: HiveConfig;
  private hiveChainFailover: HiveChainWithFailover | null = null;
  private hive: IHiveChainInterface | null = null;
  private wax: IWaxBaseInterface | null = null;
  private beekeeper: IBeekeeperInstance | null = null;
  private session: IBeekeeperSession | null = null;
  private wallet: IBeekeeperUnlockedWallet | null = null;
  private publicKey: string | null = null;
  private walletInitialized = false;

  constructor(config: HiveConfig) {
    if (!config.witnessAccount) {
      throw new Error("witnessAccount is required");
    }

    this.config = config;
  }

  async initialize(): Promise<void> {
    try {
      this.wax = await createWaxFoundation();

      // Initialize failover chain with configured RPC nodes
      const rpcNodes = this.config.rpcNodes && this.config.rpcNodes.length > 0
        ? [...this.config.rpcNodes]
        : ["https://api.hive.blog", "https://api.deathwing.me", "https://api.openhive.network"];

      this.hiveChainFailover = new HiveChainWithFailover({
        nodes: rpcNodes,
        timeout: 3000,
        maxRetries: rpcNodes.length * 2,
      });

      await this.hiveChainFailover.initialize();
      this.hive = this.hiveChainFailover.getChain();

      // Initialize beekeeper instance (keep for reuse)
      this.beekeeper = await createBeekeeper();

      // Initialize wallet and get public key
      await this.ensureWalletReady();
      
    } catch (error) {
      throw new Error(`Failed to initialize: ${error}`);
    }
  }

  /**
   * Ensures the wallet is ready for signing.
   * Reuses an existing session/wallet or creates new ones if needed.
   */
  private async ensureWalletReady(): Promise<IBeekeeperUnlockedWallet> {
    if (!this.beekeeper) {
      throw new Error("Beekeeper not initialized");
    }

    // Try to verify existing wallet is still valid
    if (this.session && this.wallet) {
      try {
        // Validate wallet is still functional
        const keys = await this.wallet.getPublicKeys();
        if (keys && keys.length > 0) {
          return this.wallet;
        }
      } catch (err) {
        // Wallet is no longer valid, need to recreate
        console.log("\x1b[33m[WARN]\x1b[0m Wallet session expired, recreating...");
        this.wallet = null;
        this.session = null;
      }
    }

    // Create a session only once (or if previous one was closed)
    if (!this.session) {
      this.session = this.beekeeper.createSession(BEEKEEPER.WALLET_NAME);
    }

    try {
      // Try to open existing wallet
      const lockedWallet: IBeekeeperWallet = await this.session.openWallet(
        BEEKEEPER.WALLET_NAME
      );
      this.wallet = await lockedWallet.unlock(
        BEEKEEPER.WALLET_PASSWORD
      );

      const publicKeys = await this.wallet.getPublicKeys();
      if (!publicKeys || publicKeys.length === 0) {
        throw new Error("No public keys found in wallet");
      }

      this.publicKey = publicKeys[0];
      
      if (!this.walletInitialized) {
        console.log("\x1b[32m[SUCCESS]\x1b[0m Using existing wallet");
        this.walletInitialized = true;
      }
      
      return this.wallet;
    } catch (openErr) {
      // Wallet doesn't exist, create it
      if (!this.config.privateKey) {
        throw new Error(
          "HIVE_SIGNING_PRIVATE_KEY is required for wallet creation"
        );
      }

      const { wallet } = await this.session.createWallet(
        BEEKEEPER.WALLET_NAME,
        BEEKEEPER.WALLET_PASSWORD,
        false
      );
      this.wallet = wallet;
      this.publicKey = await wallet.importKey(this.config.privateKey);
      
      console.log("\x1b[32m[SUCCESS]\x1b[0m Wallet created with signing key");
      this.walletInitialized = true;
      
      return this.wallet;
    }
  }

  async publishFeedPrice(): Promise<string> {
    if (!this.hive || !this.wax) {
      throw new Error(
        "FeedPublisher not initialized. Call initialize() first."
      );
    }

    try {
      const averagePrice = await priceAggregator.getAggregatedHivePrice();
      this.validatePrice(averagePrice);

      const hbdAsset = this.wax.hbdCoins(averagePrice);
      const hiveAsset = this.wax.hiveCoins(1);

      const transactionId = await this.broadcastFeedPublish(
        hbdAsset,
        hiveAsset
      );

      return transactionId;
    } catch (error) {
      throw error;
    }
  }

  private validatePrice(priceUSD: number): void {
    if (typeof priceUSD !== "number" || priceUSD <= 0 || isNaN(priceUSD)) {
      throw new Error(
        `Invalid price for exchange rate conversion: ${priceUSD}`
      );
    }
  }
  private async broadcastFeedPublish(
    baseAsset: TInternalAsset,
    quoteAsset: TInternalAsset
  ): Promise<string> {
    if (!this.hiveChainFailover) {
      throw new Error(
        "Hive connection not initialized. Call initialize() first."
      );
    }
    if (!this.publicKey) {
      throw new Error("Public key not initialized");
    }

    // Get a fresh wallet session before each broadcast to avoid timeout issues
    const wallet = await this.ensureWalletReady();

    // Use failover to execute the broadcast operation
    return await this.hiveChainFailover.executeWithFailover(
      async (chain) => {
        const witnessSetPropsData: IWitnessSetPropertiesData = {
          owner: this.config.witnessAccount,
          witnessSigningKey: this.publicKey!,
          hbdExchangeRate: {
            base: baseAsset,
            quote: quoteAsset,
          },
        };

        const witnessOperation = new WitnessSetPropertiesOperation(
          witnessSetPropsData
        );

        const tx = await chain.createTransaction();
        tx.pushOperation(witnessOperation);

        const transactionId = tx.id;
        tx.sign(wallet, this.publicKey!);

        await chain.broadcast(tx);
        return transactionId;
      },
      "feed_publish"
    );
  }
}

export function createFeedPublisher(): FeedPublisher {
  const config = loadConfig();

  return new FeedPublisher(config.hive);
}

export function getFeedInterval(): number {
  const config = loadConfig();
  return config.priceFeed.updateInterval;
}

let _feedPublisher: FeedPublisher | null = null;

export function getFeedPublisher(): FeedPublisher {
  if (!_feedPublisher) {
    _feedPublisher = createFeedPublisher();
  }
  return _feedPublisher;
}
