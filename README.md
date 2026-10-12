# net-peer-benchmark

Alat `npbench` untuk mengukur throughput / RTT / loss / jitter overlay WireGuard (user-space TCP+UDP). Bukan bagian dari binary produk net-peering; jalankan setelah tunnel sudah `connected`.

## Build

```bash
go build -o bin/npbench ./cmd/npbench
go test ./test/unit/...
```

## Penggunaan

Pastikan overlay sudah terhubung (`GET /v1/status` → `connected`). Target pakai IP overlay (`100.64.x.x`) atau Magic DNS, bukan underlay publik.

**Server** (peer yang listen):

```bash
./bin/npbench -listen :9201
```

**Edge** (target = overlay IP server):

```bash
./bin/npbench -target 100.64.0.1 -duration 10s -streams 2
# optional JSON:
./bin/npbench -target 100.64.0.1 -json
```

Laporan: UDP `packet_loss_pct`, `success_rate`, RTT min/avg/max ms, `jitter_ms` (RFC 3550), lalu TCP upload/download `throughput_bps` / Mbps. Port default `9201`. Root tidak diperlukan selama overlay sudah ter-route.
