package dev.sharad.telemetry.core;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Atomic global and per-tenant admission. The critical section contains no I/O. Only active tenants
 * are retained, bounding the map by the global capacity.
 */
public final class AdmissionController {
  public enum Reason {
    GLOBAL_CAPACITY,
    TENANT_CAPACITY,
    DRAINING
  }

  public record Snapshot(int inFlight, int activeTenants, int capacity, boolean accepting) {}

  private final int capacity;
  private final int perTenant;
  private final Map<String, Integer> tenants = new HashMap<>();
  private final CompletableFuture<Void> drained = new CompletableFuture<>();
  private int inFlight;
  private boolean accepting = true;

  public AdmissionController(int capacity, int perTenant) {
    if (capacity < 1 || perTenant < 1 || perTenant > capacity) {
      throw new IllegalArgumentException("Require 1 <= perTenant <= capacity");
    }
    this.capacity = capacity;
    this.perTenant = perTenant;
  }

  public synchronized Lease acquire(String tenant) {
    if (!accepting) throw new Rejected(Reason.DRAINING);
    if (inFlight == capacity) throw new Rejected(Reason.GLOBAL_CAPACITY);
    int tenantCount = tenants.getOrDefault(tenant, 0);
    if (tenantCount == perTenant) throw new Rejected(Reason.TENANT_CAPACITY);
    tenants.put(tenant, tenantCount + 1);
    inFlight++;
    return new Lease(this, tenant);
  }

  public CompletionStage<Void> stopAccepting() {
    boolean alreadyDrained;
    synchronized (this) {
      accepting = false;
      alreadyDrained = inFlight == 0;
    }
    if (alreadyDrained) drained.complete(null);
    return drained.minimalCompletionStage();
  }

  public synchronized Snapshot snapshot() {
    return new Snapshot(inFlight, tenants.size(), capacity, accepting);
  }

  private void release(String tenant) {
    boolean completeDrain;
    synchronized (this) {
      int count = tenants.get(tenant);
      if (count == 1) tenants.remove(tenant);
      else tenants.put(tenant, count - 1);
      inFlight--;
      completeDrain = !accepting && inFlight == 0;
    }
    // Do not invoke arbitrary completion callbacks while holding the gate monitor.
    if (completeDrain) drained.complete(null);
  }

  public static final class Lease implements AutoCloseable {
    private final AdmissionController owner;
    private final String tenant;
    private final AtomicBoolean closed = new AtomicBoolean();

    private Lease(AdmissionController owner, String tenant) {
      this.owner = owner;
      this.tenant = tenant;
    }

    @Override
    public void close() {
      if (closed.compareAndSet(false, true)) owner.release(tenant);
    }
  }

  public static final class Rejected extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final Reason reason;

    public Rejected(Reason reason) {
      super(reason.name());
      this.reason = reason;
    }

    public Reason reason() {
      return reason;
    }
  }
}
