/**
 * Dependency injection container.
 *
 * ── Why hand-rolled ────────────────────────────────────────────────────────
 * This is ~150 lines with zero dependencies. The alternatives all cost more
 * than they give at this size:
 *
 * • InversifyJS / tsyringe — require `reflect-metadata`, `experimentalDecorators`
 *   and `emitDecoratorMetadata`. That is a global TypeScript config change, a
 *   runtime polyfill loaded before anything else, and decorator metadata that
 *   silently stops working when a type is an interface (interfaces are erased,
 *   so they cannot be injection tokens). Every team using them hits that.
 * • NestJS — brings a whole application framework to get DI.
 * • No container, manual wiring — genuinely viable, and what we do at the
 *   composition root anyway. It stops scaling when a dependency four levels
 *   deep changes and every construction site must be edited by hand.
 *
 * ── The design ─────────────────────────────────────────────────────────────
 * Tokens are branded symbols, not strings or classes. A symbol cannot collide,
 * and the phantom type parameter makes `resolve(DATABASE)` return the right type
 * with no cast at the call site and no runtime reflection.
 *
 * ── The cost we accept ─────────────────────────────────────────────────────
 * Tokens must be declared explicitly — `token<Kafka>('kafka')` — rather than
 * inferred from a class. That is the price of not using decorators, and it buys
 * something back: interfaces work as tokens, so services depend on abstractions
 * rather than concrete classes (which is the actual point of DI).
 *
 * ── Where this stops working ───────────────────────────────────────────────
 * It has no async factories and no automatic disposal ordering. Both are real
 * needs — a Kafka client must connect before use and disconnect on shutdown —
 * and both are handled by the lifecycle manager instead, deliberately: mixing
 * lifecycle into a container is how containers grow into frameworks.
 *
 * It also does not detect circular dependencies at registration time, only at
 * resolution (see `resolve`). Detecting them earlier would need a full graph
 * walk on every register, which is cost for a problem the resolution-time check
 * already catches with a readable message.
 */

/**
 * A unique, type-carrying key for a dependency.
 *
 * The `__type` field never exists at runtime — it is a phantom type that makes
 * `Token<Database>` and `Token<Cache>` incompatible at compile time, so a
 * mis-wired container fails to build rather than failing at 3am.
 */
export interface Token<T> {
  readonly symbol: symbol;
  readonly description: string;
  /** Phantom. Never read, never assigned. */
  readonly __type?: T;
}

/** Create a token. The description appears in error messages and diagrams. */
export function token<T>(description: string): Token<T> {
  return { symbol: Symbol(description), description };
}

/** How many instances the container creates. */
export type Lifetime =
  /** One instance for the container's lifetime. The default, and correct for
   *  anything holding a connection pool, a socket, or a cache. */
  | 'singleton'
  /** A new instance per resolution. For stateful, short-lived objects. */
  | 'transient';

/** Resolves other dependencies during construction. */
export type Factory<T> = (container: Container) => T;

interface Registration<T> {
  readonly factory: Factory<T>;
  readonly lifetime: Lifetime;
  instance?: T;
}

/** Thrown when a token is resolved that was never registered. */
export class UnregisteredDependencyError extends Error {
  constructor(description: string) {
    super(
      `No registration for "${description}". ` +
        `Register it at the composition root before resolving.`,
    );
    this.name = 'UnregisteredDependencyError';
  }
}

/**
 * Thrown when dependencies form a cycle.
 *
 * The message includes the full path (`a -> b -> c -> a`) because a cycle is
 * almost never obvious from either end — you need to see the loop to know which
 * edge to break.
 */
export class CircularDependencyError extends Error {
  constructor(path: readonly string[]) {
    super(`Circular dependency: ${path.join(' -> ')}`);
    this.name = 'CircularDependencyError';
  }
}

/**
 * A dependency container.
 *
 * Not a service locator: application code never holds a reference to it. Only
 * the composition root does. Passing the container into a service would let
 * that service resolve anything, which hides its dependencies — exactly the
 * problem DI exists to solve.
 */
export class Container {
  readonly #registrations = new Map<symbol, Registration<unknown>>();
  /** Tokens currently being constructed, used for cycle detection. */
  readonly #resolving: symbol[] = [];
  readonly #descriptions = new Map<symbol, string>();

  /**
   * Register a factory.
   *
   * Re-registering a token replaces it. That is intentional and useful — tests
   * build the production container and override the two things they need to
   * fake, rather than reconstructing the whole graph by hand.
   */
  public register<T>(tok: Token<T>, factory: Factory<T>, lifetime: Lifetime = 'singleton'): this {
    // No cast needed: Factory<T> is assignable to Factory<unknown> because a
    // function returning T satisfies one returning unknown (return types are
    // covariant). The type safety lives in the Token's phantom parameter, which
    // is what makes resolve() give back the right type.
    this.#registrations.set(tok.symbol, { factory, lifetime });
    this.#descriptions.set(tok.symbol, tok.description);
    return this;
  }

  /** Register an already-constructed value. Config objects, clocks, fakes. */
  public registerValue<T>(tok: Token<T>, value: T): this {
    return this.register(tok, () => value, 'singleton');
  }

  /**
   * Resolve a dependency, constructing it and its transitive dependencies.
   *
   * Singletons are constructed at most once — the instance is cached before the
   * factory returns is *not* possible here (the value does not exist yet), so a
   * cycle among singletons is caught by the path check rather than producing a
   * half-built object.
   */
  public resolve<T>(tok: Token<T>): T {
    const registration = this.#registrations.get(tok.symbol) as Registration<T> | undefined;
    if (!registration) throw new UnregisteredDependencyError(tok.description);

    if (registration.lifetime === 'singleton' && registration.instance !== undefined) {
      return registration.instance;
    }

    if (this.#resolving.includes(tok.symbol)) {
      const path = [...this.#resolving, tok.symbol].map(
        (s) => this.#descriptions.get(s) ?? s.toString(),
      );
      throw new CircularDependencyError(path);
    }

    this.#resolving.push(tok.symbol);
    try {
      const instance = registration.factory(this);
      if (registration.lifetime === 'singleton') registration.instance = instance;
      return instance;
    } finally {
      // finally, not after the assignment: a factory that throws must not leave
      // its token stuck in the resolving path, or every later resolution of it
      // reports a phantom cycle.
      this.#resolving.pop();
    }
  }

  /** Resolve if registered, otherwise undefined. For genuinely optional collaborators. */
  public tryResolve<T>(tok: Token<T>): T | undefined {
    return this.#registrations.has(tok.symbol) ? this.resolve(tok) : undefined;
  }

  public has(tok: Token<unknown>): boolean {
    return this.#registrations.has(tok.symbol);
  }

  /**
   * Create a child container that inherits registrations but resolves its own
   * singleton instances.
   *
   * Used for request scoping: a child gets its own per-request context while
   * still seeing the parent's shared connection pools by reference.
   */
  public createScope(): Container {
    const child = new Container();
    for (const [symbol, registration] of this.#registrations) {
      // Copy the registration but NOT the cached instance — a scope that shared
      // parent instances would not be a scope. Values registered via
      // registerValue are shared regardless, because their factory closes over
      // the value itself.
      child.#registrations.set(symbol, {
        factory: registration.factory,
        lifetime: registration.lifetime,
      });
    }
    for (const [symbol, description] of this.#descriptions) {
      child.#descriptions.set(symbol, description);
    }
    return child;
  }

  /**
   * Eagerly construct every singleton.
   *
   * Called at the end of composition so that a mis-wired graph fails at startup
   * — where a health check catches it and the deploy rolls back — rather than
   * on the first request that happens to touch the broken branch.
   */
  public verify(): void {
    for (const [symbol, registration] of this.#registrations) {
      if (registration.lifetime !== 'singleton') continue;
      const description = this.#descriptions.get(symbol) ?? symbol.toString();
      // Reconstructing the token from the symbol is safe here: we only need it
      // to trigger construction and surface a failure, never to use the value.
      this.resolve({ symbol, description });
    }
  }

  /** Registered token descriptions. For diagnostics and startup logging. */
  public registered(): string[] {
    return [...this.#descriptions.values()].sort();
  }
}
