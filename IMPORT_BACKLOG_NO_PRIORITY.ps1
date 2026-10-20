# Import toàn bộ Product Backlog thành GitHub Issues và add vào GitHub Project.
# Không dùng Priority.
#
# 1) Cài GitHub CLI:
#    winget install --id GitHub.cli
#
# 2) Đăng nhập:
#    gh auth login
#
# 3) Cấp quyền GitHub Projects:
#    gh auth refresh -s project
#
# 4) Đặt file này cùng thư mục với BACKLOG_ISSUES_NO_PRIORITY.csv
# 5) Chạy:
#    powershell -ExecutionPolicy Bypass -File .\IMPORT_BACKLOG_NO_PRIORITY.ps1

$Repo = "dtshsgsvietnam/BigData.Nhom_9.20261"
$ProjectTitle = "Wikipedia Attention Observatory"
$CsvPath = ".\BACKLOG_ISSUES_NO_PRIORITY.csv"

gh auth status
if ($LASTEXITCODE -ne 0) { exit 1 }

gh auth refresh -s project

# Tạo các label Epic. Nếu label đã tồn tại thì --force chỉ cập nhật nó.
$labelMap = @{
    "Setup" = "epic:setup"
    "Ingestion" = "epic:ingestion"
    "Infrastructure" = "epic:infrastructure"
    "Storage" = "epic:storage"
    "Batch" = "epic:batch"
    "Attention" = "epic:attention"
    "Streaming" = "epic:streaming"
    "Serving" = "epic:serving"
    "Dashboard" = "epic:dashboard"
    "Experiment" = "epic:experiment"
    "Fault Tolerance" = "epic:fault-tolerance"
    "Documentation" = "epic:documentation"
    "Demo" = "epic:demo"
    "Presentation" = "epic:presentation"
    "Stretch" = "epic:stretch"
}

foreach ($kv in $labelMap.GetEnumerator()) {
    gh label create $kv.Value -R $Repo --description $kv.Key --force | Out-Null
}

# Lấy toàn bộ issue hiện tại để tránh tạo trùng theo PB-ID.
$existing = gh issue list -R $Repo --state all --limit 500 --json title,url | ConvertFrom-Json
$items = Import-Csv $CsvPath

foreach ($row in $items) {
    $id = $row.ID
    $title = "$id — $($row.Title)"

    $dup = $existing | Where-Object { $_.title -match "$([regex]::Escape($id))\b" }
    if ($dup) {
        Write-Host "SKIP (đã có): $title"
        continue
    }

    $label = $labelMap[$row.Epic]

    $body = @"
## Mục tiêu

$($row.Title)

## Thông tin

- **ID:** $($row.ID)
- **Epic:** $($row.Epic)
- **Owner role:** $($row.OwnerRole)
- **Tuần dự kiến:** $($row.Week)

## Definition of Done

- [ ] Hoàn thành công việc mô tả trong issue
- [ ] Đã chạy thử hoặc kiểm chứng kết quả
- [ ] Có log/output/tài liệu nếu task yêu cầu
- [ ] Không chứa secret/password
- [ ] Nếu có code: đã được review trước khi merge
"@

    Write-Host "CREATE: $title"

    gh issue create `
        -R $Repo `
        --title $title `
        --body $body `
        --label $label `
        --project $ProjectTitle

    if ($LASTEXITCODE -ne 0) {
        Write-Host "FAILED: $title"
    }
}

Write-Host ""
Write-Host "Hoàn tất import backlog."
