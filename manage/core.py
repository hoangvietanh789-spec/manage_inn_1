def say_hello(name: str) -> str:
    return f"Hello, {name}! This is manage package."
    
def run():
    import json
    import pandas as pd
    import re
    from datetime import datetime
    from datetime import timedelta
    from dateutil.relativedelta import relativedelta

    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")
    pre_month_day = today - relativedelta(months = 1)
    pre_month = datetime.strftime(pre_month_day, "%Y%m")

    from google.colab import drive
    drive.mount('/content/drive')

    file_all = "/content/drive/MyDrive/Dau_tu/data/all.json"
    file_price = "/content/drive/MyDrive/Dau_tu/data/price.json"
    file_report = "/content/drive/MyDrive/Dau_tu/report/rent_report.xlsx"
    html_path = "/content/drive/MyDrive/Dau_tu/report/rent_report.html"

    # --- Config đơn giá ---
    with open(file_price) as file:
        price = json.loads(file.read())

    electric_price = price[this_month]['electric_price']
    water_price = price[this_month]['water_price']

    # --- Load dữ liệu JSON ---
    with open(file_all, "r", encoding="utf-8") as f:
        raw = f.read()

    # Xóa dấu phẩy thừa trước } hoặc ]
    clean = re.sub(r",(\s*[}\]])", r"\1", raw)
    data = json.loads(clean)
    all_records = []

    def calculate(room,info):
        if info["electric_start"] is not None and info["electric_end"] is not None:
            electric_fee = (info["electric_end"] - info["electric_start"]) * electric_price
        else:
            electric_fee = 0

        if info["water_start"] is not None and info["water_end"] is not None:
            water_fee = (info["water_end"] - info["water_start"]) * water_price
        else:
            water_fee = 0

        rent_price = info["rent_price"] or 0
        payment = info["payment"] or 0

        bill = rent_price + electric_fee + water_fee
        due_amount = bill - payment if  bill - payment > 0 else 0

        # Cập nhật vào dict
        info["electric_fee"] = electric_fee
        info["water_fee"] = water_fee
        info["bill"] = bill
        info["due_amount"] = due_amount

        # Auto update status
        if info["start_date"]:
            info["status"] = "rented"
        else:
            info["status"] = "available"

        return(info)

    for month, rooms in data.items():
        for room, info in rooms.items():
            if month != this_month:
                info = calculate(room, info)
            all_records.append({
                "month": month,
                "room": room,
                "tenant": info["phone"],
                "rent_price": info["rent_price"],
                "electric_fee": info["electric_fee"],
                "water_fee": info["water_fee"],
                "bill": info["bill"],
                "payment": info["payment"],
                "due_amount": info["due_amount"],
                "status": info["status"],
                # 👉 Thêm cột link Zalo
                "zalo_link": f"https://zalo.me/{info['phone']}" if info.get("phone") else ""
            })

    df = pd.DataFrame(all_records)
    df.to_excel(file_report, index=False)


    # --- Lưu lại file JSON đã cập nhật ---
    with open("rent_data_updated.json", "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

    print("✅ Đã tạo rent_report.csv và rent_data_updated.json kèm cột Zalo link")



    import os
    def extract_phone(s: str):
        """Trích số điện thoại VN bắt đầu bằng 0, độ dài 9–11 số từ chuỗi bất kỳ."""
        if not s:
            return None
        m = re.search(r'(0\d{8,10})', s)
        return m.group(1) if m else None

    def fmt(x):
        """Định dạng số có dấu phẩy, trả '0' nếu None/False."""
        try:
            return f"{(0 if x in [None, ''] else x):,.0f}"
        except Exception:
            return "0"

    # Tạo từng hàng HTML
    rows_html = []
    for rec in all_records:
        phone = extract_phone(rec.get("tenant"))
        zalo_html = f'<a href="https://zalo.me/{phone}" target="_blank" class="zalo-button">Zalo</a>' if phone else "-"

        rows_html.append(f"""
          <tr>
            <td>{rec.get('month','')}</td>
            <td>{rec.get('room','')}</td>
            <td>{rec.get('tenant','')}</td>
            <td>{fmt(rec.get('rent_price'))}</td>
            <td>{fmt(rec.get('electric_fee'))}</td>
            <td>{fmt(rec.get('water_fee'))}</td>
            <td>{fmt(rec.get('bill'))}</td>
            <td>{fmt(rec.get('payment'))}</td>
            <td>{fmt(rec.get('due_amount'))}</td>
            <td>{rec.get('status','')}</td>
            <td>{zalo_html}</td>
          </tr>
        """)

    HTML_TEMPLATE = f"""<!doctype html>
    <html lang="vi">
      <head>
        <meta charset="utf-8">
        <title>Báo cáo Quản lý Nhà trọ</title>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
          body {{ font-family: Arial, sans-serif; margin: 20px; }}
          h2 {{ margin-top: 0; }}
          table {{ border-collapse: collapse; width: 100%; margin-bottom: 16px; }}
          th, td {{ border: 1px solid #ddd; padding: 8px; text-align: center; }}
          th {{ background-color: #f2f2f2; position: sticky; top: 0; }}
          tr:nth-child(even) {{ background: #fafafa; }}
          .zalo-button {{
              display: inline-block;
              background-color: #0068ff;
              color: white;
              padding: 6px 12px;
              border-radius: 6px;
              text-decoration: none;
              font-weight: bold;
              font-size: 13px;
          }}
          .zalo-button:hover {{ background-color: #0050cc; }}
          .note {{ color: #666; font-size: 12px; }}
          .wrap {{ overflow-x: auto; }}
        </style>
      </head>
      <body>
        <h2>Báo cáo Quản lý Nhà trọ</h2>
        <div class="wrap">
          <table>
            <thead>
              <tr>
                <th>Tháng</th>
                <th>Phòng</th>
                <th>Người thuê</th>
                <th>Giá thuê</th>
                <th>Tiền điện</th>
                <th>Tiền nước</th>
                <th>Tổng bill</th>
                <th>Đã trả</th>
                <th>Còn nợ</th>
                <th>Trạng thái</th>
                <th>Liên hệ</th>
              </tr>
            </thead>
            <tbody>
              {''.join(rows_html)}
            </tbody>
          </table>
        </div>
        <div class="note">* Nút Zalo tự trích số điện thoại từ cột "Người thuê" nếu có.</div>
      </body>
    </html>
    """

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(HTML_TEMPLATE)

    print(f"✅ Đã tạo file HTML: {html_path}")

def view():
    from IPython.display import HTML
    url = "https://sites.google.com/view/trosupham2vietxocodien"
    return(HTML(f'<a href="{url}" target="_blank">👉 Mở trang web</a>'))
