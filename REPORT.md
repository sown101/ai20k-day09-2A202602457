# REPORT — Lab Day 9

> Điền **bằng lời và số liệu của chính bạn** (lấy từ đầu ra của `scripts/lab.sh step ...` và từ `report/evidence/*.json`). Bài làm công khai trên GitHub; giảng viên đối chiếu với lịch sử commit và với bài của các bạn khác. Mỗi câu 3 đến 8 dòng là đủ.

| | |
|---|---|
| Họ và tên | Nguyễn Hoàng Sơn |
| MSSV (đúng bằng `LAB_STUDENT_ID` trong `.env`) | `2A202602457` |
| GitHub | `sown101` |
| Trip dùng cho bài tập sập và chạy lại (xem GUIDE, bước 5) | `trip-04868` |

## Kết quả cuối

- `dataset_version` của bạn: `40d43c57c320`  (phải khớp `report/evidence/run_summary.json`)
- Số khung hình train / val / test của bạn: **683 / 144 / 192**, tổng **1019 khung**, thuộc **18 / 4 / 4 trip**.
- `report/verify.txt`: đầu ra thực tế của `docker compose exec -T -w /opt/lab airflow /home/airflow/lab-venv/bin/python -m pipeline.cli verify`, đạt **27/27 checks passed**. Dùng lệnh Docker trực tiếp để lưu trên PowerShell vì `--save-verify` của script gọi shell Bash.
- Ảnh chụp trong `report/screenshots/`: `airflow_graph.png`, `minio_lake.png`, `metabase_dashboard.png`, `airflow_crash_graph.png`, `airflow_crash_log.png`, `airflow_recovered_graph.png`.
- Hai run Airflow baseline và phục hồi đều đạt **37/37 task success**; bộ unit test đạt **9/9**. Dashboard: [Day 9: video data pipeline](http://localhost:3000/dashboard/2).

## Bước 1 · Bronze

### Câu 1
Liệt kê **ít nhất 4 khác biệt** giữa telemetry nguồn A và nguồn B mà bạn thấy ở bronze. Vì sao pipeline giữ **mỗi nguồn một bảng bronze** và để mọi cột ở dạng chữ, thay vì gộp và ép kiểu ngay?

- Thời gian: A dùng `ts_utc` dạng ISO UTC, ví dụ `2025-10-07T08:00:00.014Z`; B dùng `timestamp_ms` dạng epoch mili giây, ví dụ `1759933801509`.
- Tốc độ: A dùng `speed_mps` theo m/s, còn B dùng `spd_kmh` theo km/h; hai dòng mẫu lần lượt có giá trị `1.213` và `55.11`.
- Hướng và vị trí: A dùng `heading_deg`, `lat`, `lon`, `alt_m`; B dùng `hdg`, `latitude`, `longitude`, `altitude`.
- Bàn đạp và điều khiển: A dùng `gas`, `brake` theo thang 0–1 và `steer`; B dùng `throttle_pct`, `brake_pct` theo phần trăm và `steering`. Gia tốc cũng đổi tên từ `acc_x`, `acc_y` sang `ax`, `ay`.
- Kết quả chạy của tôi: `raw_telemetry_a` có 3021 dòng, `raw_telemetry_b` có 3234 dòng, `trips_raw` có 26 dòng và `video_files` có 2 dòng; kiểm tra module 1 đạt **5/5**. Lần nạp lại A báo `already present` cho cả video và telemetry, số dòng không tăng.
- Mỗi nguồn có bảng riêng để giữ schema và giá trị gốc, tránh gộp nhầm các cột có tên hoặc đơn vị khác nhau. Các cột dữ liệu CSV được giữ dạng chữ để không mất thông tin khi ép kiểu; các cột truy vết như `_row_no`, `_ingested_at` vẫn có kiểu riêng. Việc chuẩn hóa được thực hiện ở Silver, còn Bronze giúp đối chiếu và xử lý lại khi quy tắc thay đổi.

## Bước 2 · Silver telemetry

### Câu 2
Pipeline đưa hai nguồn về một schema như thế nào (đơn vị, kiểu thời gian, **độ lệch đồng hồ**)? Bằng chứng nào trong kết quả bước 2 cho thấy đã đúng? Dán bảng quarantine của bạn (số dòng theo từng lý do, từng drive) và giải thích vì sao dòng lỗi vào `rejects` thay vì bị xoá.

Pipeline đổi tên các cột của B theo schema A và ép các trường số về kiểu số thực; tốc độ B được chia cho 3,6 để đổi từ km/h sang m/s, còn bàn đạp phần trăm được chia cho 100 về thang 0–1. A đã dùng các đơn vị này nên chỉ cần ép kiểu.
Thời gian A được chuyển từ chuỗi ISO sang `timestamptz`; B lấy epoch mili giây trừ độ lệch đồng hồ 1500 ms rồi chia cho 1000 để chuyển thành timestamp cùng đồng hồ UTC với video.
Sau chuẩn hóa, B bắt đầu lúc `2025-10-08 14:30:00.009000+00:00`, chỉ sau mốc bắt đầu video 9 ms; kiểm tra độ lệch đồng hồ đạt PASS. Tốc độ tối đa của A là 71 km/h, B là 100 km/h; toàn bộ tốc độ Silver nằm trong khoảng 0–70 m/s và không còn trùng `(drive_id, ts)`.

| drive_id | duplicate | speed_missing | speed_out_of_range | Tổng cách ly |
|---|---:|---:|---:|---:|
| drive_a | 20 | 30 | 3 | 53 |
| drive_b | 20 | 30 | 3 | 53 |

Silver có 6149 dòng sạch và 106 dòng trong `telemetry_rejects`: A có `2968 + 53 = 3021`, B có `3181 + 53 = 3234`, khớp đầy đủ dữ liệu Bronze. Module 2 đạt **7/7**; `dbt test` có 6 PASS, 1 WARN ở `max_gap_ms` và không có ERROR.
Dòng lỗi được giữ cùng lý do trong `rejects` để truy vết, điều tra chất lượng nguồn và xử lý lại khi sửa dữ liệu hoặc quy tắc; xóa chúng sẽ mất bằng chứng và không thể đối chiếu đủ số dòng đã nhận.

### Câu 3
Vì sao test `max_gap_ms` là `warn` còn `in_range` là `error`? Kết quả thí nghiệm `break-gate` của bạn (`break_gate.json`): test nào bắt được, bao nhiêu dòng, và điều đó cho biết gì về vai trò của "cổng chất lượng"?

Test `max_gap_ms` cảnh báo khi hai mẫu liên tiếp trong một drive cách nhau quá 1000 ms: cảm biến có thể mất tín hiệu nhưng các mẫu còn lại vẫn dùng được, nên mức `warn` báo vấn đề mà không dừng pipeline.
Test `in_range` yêu cầu tốc độ nằm trong 0–70 m/s và có mức `error`, vì tốc độ ngoài miền hợp lệ không được phép đi tiếp vào dữ liệu phục vụ huấn luyện.
Trong thí nghiệm của tôi, nới luật làm sạch lên 1000 m/s khiến test `in_range_silver_telemetry_speed_mps__70__0` bắt được **6 dòng**; kết quả là `PASS=5 WARN=1 ERROR=1 SKIP=0`.
Sau khi khôi phục ngưỡng 70 m/s, kết quả trở lại `PASS=6 WARN=1 ERROR=0 SKIP=0`. File `report/evidence/break_gate.json` ghi đúng MSSV `2A202602457`, `gate_failed: true` và số dòng lỗi là 6.
Điều này cho thấy cổng chất lượng kiểm tra đầu ra độc lập với luật làm sạch: dù luật bị nới sai, test vẫn phát hiện dữ liệu vi phạm và báo lỗi để chặn các bước tiếp theo. WARN về khoảng mất tín hiệu vẫn được giữ để theo dõi chất lượng nguồn.

## Bước 3 · Silver frames

### Câu 4
Dựa vào `idempotency.json`: số dòng trước và sau hai lần chạy lại, và khi ghi kiểu `append`. Vì sao Delta `overwrite` theo partition (một trip) an toàn hơn xoá rồi ghi lại, nhất là khi giữa chừng có lỗi?

Theo `report/evidence/idempotency.json`, tôi chạy lại `trip-00003` hai lần: toàn bảng vẫn có **1246 dòng** trước và sau, dấu vân tay vẫn là `6c7480dfde7e`, và `unchanged` là `true`.
Trip này có 60 khung; khi thử ghi `append` hai lần vào bảng thử riêng, số dòng thành **120**, tức nhân đôi. Ghi lại bằng `overwrite` theo điều kiện `trip_id` thay thế dữ liệu của đúng trip thay vì thêm một bản nữa.
Trong mã thực tế, bảng phân vùng vật lý theo `drive_id`; phạm vi thay thế được giới hạn bởi predicate `trip_id = 'trip-00003'`, nên các trip khác vẫn được giữ.
Delta công bố thay đổi bảng qua một commit nguyên tử: nếu lỗi trước khi commit thành công, phiên bản bảng đã commit trước đó vẫn đọc được. Cách xóa dữ liệu cũ rồi ghi lại bằng hai thao tác riêng có thể để trip bị thiếu dữ liệu nếu lỗi xảy ra giữa chừng.
Thí nghiệm chứng minh số dòng và dấu vân tay nội dung được kiểm tra không đổi khi chạy lại; nó không có nghĩa mọi trường hoặc lịch sử phiên bản đều không đổi, vì `_processed_at` có thể được cập nhật và Delta có thêm commit.

### Câu 5
Khung hình **đen** và khung hình **tối** khác nhau thế nào, pipeline xử lý khác nhau ra sao, bạn có đồng ý không? Dựa vào `threshold.json`: nếu chọn ngưỡng 0, 2, 6 thì bao nhiêu khung bị coi là trùng ở mỗi drive? Bạn chọn ngưỡng nào, vì sao, và ngưỡng đó phụ thuộc vào tốc độ xe thế nào?

| dHash khác tối đa | số khung trùng, drive A | số khung trùng, drive B |
|---|---|---|
| 0 | 61 | 8 |
| 2 | 149 | 53 |
| 6 | 363 | 244 |

Khung đen có độ sáng trung bình dưới 12; khung tối có độ sáng từ 12 đến dưới 40 và vẫn có thể chứa thông tin cảnh đường hầm hoặc bóng râm. Pipeline gắn cờ cả hai nhưng chỉ cờ `is_black` là điều kiện loại khỏi Gold; `is_dark` tự nó không khiến khung bị loại.
Tôi đồng ý giữ khung tối để dataset phản ánh điều kiện thiếu sáng thực tế; tuy nhiên, khung tối vẫn bị loại nếu đồng thời mờ, gần trùng hoặc mất đồng bộ với telemetry.
Bước 3 của tôi xử lý 26 trip, tổng 1246 khung: A có 602 khung, gồm 6 đen, 9 mờ, 4 tối, 149 gần trùng và 7 mất đồng bộ; B có 644 khung, gồm 4 đen, 0 mờ, 61 tối, 53 gần trùng và 7 mất đồng bộ. Các cờ có thể chồng nhau nên không cộng trực tiếp để tính số khung bị loại.
Module 3 đạt **6/6**, `dbt test` đạt 8 PASS và không có WARN hoặc ERROR. Độ trễ đồng bộ trung bình A/B là 66/73 ms, nhưng tối đa 2884/3112 ms; vì vậy độ trễ trung bình nhỏ không có nghĩa mọi khung đều đồng bộ tốt.
Theo `report/evidence/threshold.json`, ngưỡng 0/2/6 lần lượt gắn cờ 61/149/363 khung ở A và 8/53/244 khung ở B. Tôi chọn ngưỡng **2** cho bài lab: ngưỡng 0 chỉ bắt dHash giống hệt, còn ngưỡng 6 loại nhiều hơn và có nguy cơ bỏ cả cảnh khác nhau có giá trị; các số đếm này chưa đủ chứng minh ngưỡng nào tối ưu nếu chưa xem ảnh.
Với cùng tốc độ lấy mẫu 2 khung/giây, xe chậm thường tạo các khung liền nhau giống nhau hơn; xe nhanh khiến cảnh thay đổi nhiều hơn. Vì vậy cần xem ảnh và phân bố khoảng cách dHash theo tốc độ để hiệu chỉnh ngưỡng, tránh coi các khung thay đổi có ý nghĩa là trùng chỉ vì nâng ngưỡng quá cao.

## Bước 4 · Gold

### Câu 6
Dựa vào `leakage.json`: chia theo khung hình làm bao nhiêu trip bị chia cắt, so với chia theo trip? Vì sao điều này làm điểm kiểm thử đẹp một cách giả tạo?

Thí nghiệm trên 1019 khung thuộc 26 trip ghi trong `report/evidence/leakage.json`: chia theo khung làm **26 trip** nằm ở nhiều split, và cả **26 trip** đều có khung ở train lẫn test. Chia theo trip của Gold làm **0 trip** bị chia cắt.
Các khung liền nhau thường có cảnh, vị trí và điều kiện chiếu sáng gần giống nhau. Nếu khung của cùng trip xuất hiện ở cả train và test, mô hình có thể nhận lại cảnh đã thấy thay vì chứng minh khả năng tổng quát hóa sang chuyến đi mới.
Điểm test khi đó có thể cao giả tạo do hai tập không độc lập về cảnh. Chia theo trip giảm kiểu rò rỉ này, nhưng không tự bảo đảm các trip khác nhau không quay cùng tuyến đường; đánh giá thực tế còn có thể cần tách theo tuyến, ngày hoặc xe.

### Câu 7
Vì sao `dataset_version` là **băm nội dung** chứ không phải ngày giờ hay số đếm? Hai lần chạy liên tiếp cho kết quả gì? Vì sao `dataset_version` của bạn **khác** bạn bè dù cùng dữ liệu (liên hệ MSSV)?

Pipeline sắp các khung theo `frame_id`, băm SHA256 chuỗi các cặp `frame_id|split` rồi lấy 12 ký tự đầu làm `dataset_version`. Vì vậy cùng tập khung và cách chia split sẽ cho cùng version, thay vì đổi tên chỉ vì thời gian chạy hoặc số lượt chạy tăng.
Đây là băm danh sách khung và split, không phải băm toàn bộ byte ảnh hay mọi trường telemetry; nếu chỉ những nội dung đó thay đổi thì version này chưa chắc đổi.
Lần chạy bước 4 của tôi tạo version `40d43c57c320`, có 1019 khung: train 683, val 144, test 192; module 4 đạt **7/7** và `dbt test` đạt 7 PASS. Mỗi drive có 9 trip train, 2 val, 2 test; tỷ lệ khung không nhất thiết đúng 70/15/15 vì đơn vị chia là trip.
Chạy bước 4 lần thứ hai vẫn cho version `40d43c57c320`, tổng 1019 khung và train/val/test 683/144/192; các kiểm tra vẫn đạt 7/7. Việc chạy lại không làm đổi danh sách khung hoặc cách chia split.
MSSV `2A202602457` được ghép với `trip_id` trước khi băm MD5 để xếp thứ tự trip trong từng drive. MSSV khác thường tạo cách chia khác, dẫn đến danh sách `frame_id|split` và version khác; tuy nhiên không phải bảo đảm toán học rằng mọi MSSV khác đều cho version khác.
Tôi thử MSSV `2A202602458` trong môi trường của một lệnh: version đổi thành `fbfdfc83ec8a`, train/val/test thành 667/129/223 dù tổng vẫn là 1019. Khôi phục MSSV gốc cho lại `40d43c57c320` và 683/144/192; bằng chứng trong `report/evidence/version_check.json`.

### Câu 8
Đọc phần `lineage` trong `manifest.json` của dataset bạn. Trả lời "dataset này được tạo từ dữ liệu nào?" bằng chính nội dung đó. Nếu một người hỏi 3 tháng sau, thông tin nào giúp tái tạo lại đúng dataset này?

Tôi đọc `s3://lake/gold/datasets/40d43c57c320/manifest.json` ở lần chạy toàn bộ cuối, đồng thời lưu bản sao tại `report/evidence/manifest.json`: dataset gồm 436 khung của A và 583 khung của B, tổng 1019 khung từ 26 trip.
Lineage ghi các phiên bản Delta đầu vào: `bronze.raw_telemetry_a = 0`, `bronze.raw_telemetry_b = 0`, `bronze.trips_raw = 0`, `bronze.video_files = 1`; `silver.frames = 25`, `silver.telemetry = 0`, `silver.frames_flagged = 0`, `silver.frame_telemetry = 0`.
SHA256 video A là `636ba1a98dfc5ba665d1c935e1d2064e69f4df360ff9ffbd15795957d5e7e9aa`; video B là `bfa62703f645ff77a8578ae3fc74a00881140d38a404d43c8e2af265bbee9fdc`. Các mã này giúp xác nhận đúng tệp video nguồn, không chỉ cùng tên tệp.
Manifest ghi luật loại `is_black`, `is_blurry`, `is_near_dup`, `not sync_ok`, và chia theo trip, phân tầng theo drive, xếp thứ tự MD5. `manifest_sha256` là `c56ee3fc9844f837172a45d1476e712ba299634eab1b50a77838564c79da02e5`, dùng để đối chiếu file `manifest.parquet` của lần xuất này.
Để tái tạo sau 3 tháng, cần lưu dữ liệu nguồn cùng checksum, các snapshot Delta và transaction log đúng phiên bản, manifest, mã nguồn/cấu hình và môi trường chạy, cùng MSSV dùng chia split. Lineage chỉ chỉ ra nguồn và phiên bản; nếu dữ liệu cũ đã bị xóa thì thông tin này một mình không thể khôi phục chúng.

## Bước 5 · Điều phối và phục vụ

### Câu 9
Mô tả chính xác điều xảy ra khi trip của bạn sập rồi chạy lại: bao nhiêu task chuyển đỏ, bao nhiêu task `upstream_failed`, bạn clear gì, dữ liệu có nhân đôi không, `dataset_version` có đổi không? Bằng chứng (ảnh, số) là gì?

Theo quy tắc lấy 5 số cuối MSSV, `int("02457") % 26 = 13`; phần tử chỉ số 13 của danh sách trip sắp xếp là **`trip-04868`**. Tôi đặt `LAB_FAIL_ONCE=trip-04868`, chạy DAG reset rồi trigger pipeline.
Run `manual__day09_crash_20261012T143830Z` kết thúc với **29 task success, 1 failed, 7 upstream_failed**. Task đỏ là `silver_frames`, map index 13; log có `simulated crash while extracting trip-04868; run it again`. Trong mã, lỗi mô phỏng xảy ra trước bước giải mã, nên bài tập này không chứng minh lỗi giữa một commit Delta.
Lần đầu tôi chỉ clear task frame bị lỗi: task đó chạy lại thành công, 25 task frame còn lại không chạy lại, nhưng DAG vẫn failed với **30 success và 7 upstream_failed**. Đối chiếu thời điểm bắt đầu task trước/sau xác nhận chỉ `silver_frames[13]` được thực thi lại ở lần clear này.
Tôi tiếp tục clear 7 task phụ thuộc đã upstream_failed: `dbt_run_silver`, `dbt_test_silver`, `dbt_run_gold`, `dbt_test_gold`, `publish_lake`, `export_dataset`, `publish_warehouse`. Chúng được chạy sau khi trip đã thành công, đưa cùng run về **37/37 task success**; cần clear downstream trong lần chạy thực tế này, khác mô tả rút gọn của GUIDE.
Sau phục hồi, verify đạt **27/27**, Silver vẫn có 1246 frame_id duy nhất và Gold có 1019 khung với version **`40d43c57c320`**, khớp baseline trước lỗi. Tôi đã xóa `LAB_FAIL_ONCE` sau bài tập để lần chạy sau không còn bật mô phỏng lỗi.
Bằng chứng: `report/evidence/airflow_recovery.json`, `airflow_crash_log.txt`, `report/verify.txt`; ảnh `airflow_crash_graph.png`, `airflow_crash_log.png`, `airflow_recovered_graph.png` trong `report/screenshots/`.

### Câu 10
Nâng từ 2 drive lên **500 xe mỗi ngày**: ba thành phần nào của pipeline này sẽ vỡ đầu tiên, và bạn thay bằng gì (nối với slide 12, 22, 23)?

Đây là dự đoán dựa trên cấu hình lab, chưa phải kết quả đo tải 500 xe; thứ tự nghẽn thực tế còn phụ thuộc thời lượng video, CPU, RAM và băng thông. Cần mở rộng riêng các giai đoạn xử lý, lưu trữ và phục vụ dữ liệu thay vì tăng mọi thứ trong một container.

1. **Giải mã video và worker:** `silver_frames` chỉ chạy một task map cùng lúc, Airflow dùng LocalExecutor với parallelism 2 và một container giới hạn 2500 MB. Tôi tách scheduler/webserver khỏi worker, dùng hàng đợi và các worker CPU cho FFmpeg theo trip, có giới hạn tài nguyên và cơ chế chạy lại idempotent; CeleryExecutor hỗ trợ mở rộng worker trên nhiều máy. [Tài liệu Airflow](https://airflow.apache.org/docs/apache-airflow-providers-celery/stable/celery_executor.html).
2. **Biến đổi dữ liệu trên DuckDB cục bộ:** mọi bước dbt dùng chung `work/lakehouse.duckdb`; nhiều tiến trình ghi vào một file và lượng dữ liệu vượt RAM sẽ hạn chế khả năng xử lý đồng thời. Tôi chuyển các phép biến đổi lớn sang engine phân tán như Spark trên bảng Delta ở object storage, giữ DuckDB cho khám phá mẫu; cần đo tải trước khi chọn cụm. [Mô hình ghi đồng thời của DuckDB](https://duckdb.org/docs/stable/connect/concurrency).
3. **Lưu trữ và xuất dữ liệu:** mỗi khung tạo một JPEG, mỗi trip có một commit Delta; Gold sao chép ảnh và warehouse thay cả bảng mart khi xuất. Tôi dùng object storage có khả năng mở rộng, gom lô ghi và compact các file Parquet nhỏ, tránh phân vùng quá nhỏ; mart được cập nhật tăng dần theo ngày/xe và chỉ phục vụ các bảng cần cho dashboard. Compaction áp dụng cho file bảng Delta, không tự gom các JPEG thành một file. [Hướng dẫn Delta Lake](https://docs.delta.io/best-practices/).

## Câu hỏi riêng của bạn trên Metabase

Câu SQL bạn thêm vào dashboard và điều nó cho biết:

Tôi thêm câu hỏi **"2A202602457: Trip nào có tỷ lệ khung bị loại cao nhất?"** vào [dashboard](http://localhost:3000/dashboard/2), [câu hỏi riêng](http://localhost:3000/question/46):

```sql
SELECT drive_id, trip_id, frames, frames_kept,
       frames - frames_kept AS frames_dropped,
       ROUND(100.0 * (frames - frames_kept) / NULLIF(frames, 0), 2) AS drop_pct
FROM marts.mart_trip_summary
ORDER BY drop_pct DESC, drive_id, trip_id;
```

`trip-04622` của A có tỷ lệ loại cao nhất: 34/60 khung (**56,67%**), chỉ giữ 26 khung; tiếp theo là `trip-03904` với 11/22 (**50%**). Trip bài tập `trip-04868` có 60 khung, giữ 49, loại 11 (**18,33%**). Tỷ lệ được tính từ tổng khung trừ khung Gold để tránh đếm trùng các cờ chất lượng. Truy vấn giúp ưu tiên kiểm tra trip mất nhiều dữ liệu, nhưng chưa tự xác định nguyên nhân cụ thể; kết quả đầy đủ lưu trong `report/evidence/metabase_question.json`.

## Nhìn lại

Điều bạn thấy bất ngờ nhất, điều gì còn chưa rõ, và một lỗi dữ liệu mà bạn nghĩ pipeline **chưa** bắt được (nếu có). Tối đa 8 dòng.

Kết quả đáng chú ý là B có 61 khung tối nhưng không bị loại chỉ vì thiếu sáng; giữ những cảnh này giúp dữ liệu phản ánh đường hầm và bóng râm thực tế.
Thí nghiệm leakage cho thấy toàn bộ 26 trip bị chia cắt khi chia theo khung, dù vẫn có đủ ba split; kiểm tra đủ train/val/test một mình chưa đủ bảo đảm chất lượng đánh giá.
Điểm cần tìm hiểu thêm là cách chọn ngưỡng dHash theo tốc độ và cảnh: các số đếm cho thấy ngưỡng 6 loại nhiều hơn, nhưng chưa có nhãn đối chiếu để kết luận khung nào bị loại sai.
Một lỗi có thể chưa được bắt là tọa độ GPS sai nhưng vẫn là số hợp lệ: kiểm tra tốc độ nằm trong miền 0–70 m/s không bảo đảm vị trí hoặc quỹ đạo đúng. Có thể bổ sung giới hạn lat/lon và kiểm tra độ nhảy vị trí theo thời gian.
