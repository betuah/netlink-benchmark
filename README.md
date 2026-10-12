# net-peer-benchmark

Alat `npbench` untuk mengukur throughput / RTT / loss / jitter overlay WireGuard (user-space TCP+UDP), plus **stream stability** (UDP CBR seperti video) dan **path MTU discovery**. Bukan bagian dari binary produk net-peering; jalankan setelah tunnel sudah `connected`.

## Build

```bash
go build -o bin/npbench ./cmd/npbench
go test ./...
```

## Penggunaan

Pastikan overlay sudah terhubung (`GET /v1/status` → `connected`). Target pakai IP overlay (`100.64.x.x`) atau Magic DNS, bukan underlay publik.

**Server** (peer yang listen) — satu proses melayani semua mode client:

```bash
./bin/npbench -listen :9201
```

### Throughput (default)

UDP latency probes, lalu TCP upload/download:

```bash
./bin/npbench -target 100.64.0.1 -duration 10s -streams 2
./bin/npbench -target 100.64.0.1 -duration 10s -streams 2 -direction bidir
./bin/npbench -target 100.64.0.1 -json
```

### Stream stability (video / RTSP-like)

`npbench` **tidak** membuka sesi RTSP dan **tidak** berbicara protokol kamera/NVR. Mode ini hanya mengirim UDP constant-bitrate (CBR) di jalur overlay — pola traffic yang mirip RTP/UDP video (mis. chunk MPEG-TS 7×188 = 1316 byte). Pakai sebelum deploy kamera/NVR lewat tunnel untuk melihat loss, jitter, burst, dan paket “late”.

```bash
# CBR 5 Mbit/s selama 30s, paket 1316 byte (default)
./bin/npbench -target 100.64.0.1 -mode stream -bitrate 5M -duration 30s

# Bitrate lain / paket lebih aman di bawah MTU overlay
./bin/npbench -target 100.64.0.1 -mode stream -bitrate 2M -packet 1200 -duration 60s -late-ms 150
```

**Verdict** (lihat juga laporan):

| Verdict | Loss | Jitter (RFC 3550) | Late (echo RTT > `-late-ms`) | Max loss burst |
|---------|------|-------------------|------------------------------|----------------|
| `stable` | ≤ 0.5% | ≤ 20 ms | ≤ 1% | ≤ 3 packets |
| `degraded` | ≤ 2% | ≤ 50 ms | ≤ 5% | ≤ 10 packets |
| `unsuitable` | di luar itu | | | |

`late` memakai echo RTT (server memantulkan header saja) sebagai proksi delay jalur di bawah beban; untuk playback, late ≈ lost. Server lama yang belum paham handshake stream akan gagal dengan pesan jelas (upgrade `npbench` di kedua sisi).

### Path MTU

```bash
./bin/npbench -target 100.64.0.1 -mode mtu
```

Client binary-search ukuran datagram UDP dengan **Don't Fragment** bila OS mengizinkan (Linux: `IP_MTU_DISCOVER` / `IPV6_MTU_DISCOVER`; macOS: `IP_DONTFRAG` pada socket `udp4`; Windows: `IP_DONTFRAGMENT`). Tanpa DF, hasil ditandai `soft_probe` (kurang definitif). Tidak perlu root di platform umum; bila sockopt gagal, laporan menjelaskan fallback.

Perkiraan IP MTU = UDP payload + 28 (IPv4) atau + 48 (IPv6). Bandingkan dengan harapan overlay WireGuard **1420** (direct) / **1414** (relay, −6 byte).

## Flag

| Flag | Default | Mode | Deskripsi |
|------|---------|------|-----------|
| `-listen` | kosong | listen | Alamat TCP+UDP yang di-listen (mis. `:9201`). Jika diisi, npbench jalan sebagai server; kosong = client. |
| `-target` | kosong (wajib) | client | Host atau `host:port` peer (IP overlay / Magic DNS). Tanpa port → `9201`. |
| `-mode` | `throughput` | client | `throughput` (latency UDP + TCP), `stream` (UDP CBR), `mtu` (path MTU). |
| `-duration` | `10s` | throughput / stream | Lama transfer TCP **per arah**, atau lama kirim CBR di mode stream. |
| `-streams` | `1` | throughput | Jumlah koneksi TCP paralel **per arah**. |
| `-payload` | `65536` | throughput | Ukuran chunk tiap `write`/`read` TCP (byte); server memakai nilai yang sama, maks 4 MiB. |
| `-packet` | `1316` | stream | Ukuran datagram UDP CBR (byte). 1316 = 7×188 MPEG-TS; 1200 lebih aman di bawah MTU overlay. |
| `-bitrate` | `5M` | stream | Target CBR: `5M`, `5000k`, `5Mbps`, atau bps mentah. |
| `-late-ms` | `150` | stream | Echo RTT di atas nilai ini dihitung sebagai late (playback budget). |
| `-direction` | `both` | throughput | `upload`, `download`, `both` (berurutan), `bidir` (bersamaan). |
| `-probes` | `50` | throughput | Jumlah probe UDP echo untuk RTT / loss / jitter. |
| `-probe-interval` | `20ms` | throughput | Jeda antar probe UDP. |
| `-json` | `false` | client | Stdout hanya JSON; progress mati kecuali `-progress`. |
| `-quiet` | `false` | listen + client | Client: tanpa progress. Listen: tanpa baris per sesi. |
| `-progress` | `false` | client | Tetap tampilkan progress di stderr walau `-json`. |
| `-no-color` | `false` | client | Laporan tanpa warna (juga mati jika `NO_COLOR` di-set, `TERM=dumb`, stdout bukan TTY, atau Windows). |

**`-streams N`** membuka N koneksi TCP sekaligus untuk setiap arah yang diukur, lalu menjumlahkan byte-nya (sama seperti `iperf3 -P N`). `both` dengan `-streams 2` = 2 koneksi upload, lalu 2 koneksi download; `bidir` dengan `-streams 2` = 4 koneksi aktif bersamaan (2 upload + 2 download). Satu koneksi TCP dibatasi oleh window/RTT dan langsung menurunkan laju saat ada loss; di jalur dengan latency tinggi atau loss, beberapa koneksi paralel mengisi pipa lebih penuh, sehingga hasilnya lebih dekat ke kapasitas link. Di LAN atau loopback, 1–2 stream biasanya sudah cukup.

Di mode `throughput`, fase UDP latency selalu jalan dulu, baru fase TCP. Di `bidir` semua koneksi di-dial dulu, lalu semua stream mulai dari titik start yang sama. Server tidak perlu mode khusus: stream/mtu memakai tipe pesan UDP baru; client lama tetap kompatibel untuk throughput.

## Progress & laporan

Selama berjalan, progress ditulis ke **stderr** (stdout tetap bersih). Di terminal, satu baris di-update di tempat tiap ±250 ms lalu dihapus sebelum laporan:

```
⠹ latency   sent 24/50  recv 24  loss 0.0%  rtt 0.34 ms  jitter 0.04 ms
⠙ upload    1.2s/3.0s  3.72 GiB  now 26510.5 Mbps  avg 26511.1 Mbps
⠼ stream   12.3s/30.0s  sent 5840  recv 5835  loss 0.1%  jitter 2.3 ms  4.98 Mbps
⠸ mtu      trying 1400  ok 1280  hi 1499
```

Laporan akhir di **stdout** (muat di 80 kolom). Mode stream menampilkan section **Stream Stability (UDP CBR)** plus appendix per-detik; mode mtu menampilkan UDP payload MTU, estimasi IP MTU, DF/soft probe, dan risiko fragmentasi vs 1420/1414.

Warna: packet loss hijau = 0, kuning < 2 %, merah ≥ 2 %; jitter hijau < 5 ms, kuning < 30 ms, merah ≥ 30 ms; verdict stream hijau/kuning/merah sesuai tabel di atas.

Mode `-listen` mencetak satu baris per sesi ke stderr (bukan per paket), misalnya:

```
npbench: session remote=100.64.0.7:53889 phase=upload bytes=10765467648 elapsed=3.00s mbps=28707.88
npbench: session remote=100.64.0.7:53890 phase=stream udp started
npbench: session remote=100.64.0.7:53890 phase=mtu udp started
```
