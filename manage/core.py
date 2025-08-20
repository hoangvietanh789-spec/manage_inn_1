db_file = "/content/drive/MyDrive/Dau_tu/data/inn.db"
def run():
    import json
    import pandas as pd
    from datetime import datetime

    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")

    from google.colab import drive
    drive.mount('/content/drive')

    file_price = "/content/drive/MyDrive/Dau_tu/data/price.json"
    file_all = "/content/drive/MyDrive/Dau_tu/data/all.json"
    file_report = "/content/drive/MyDrive/Dau_tu/report/rent_report.xlsx"

    price = query('prices')
    electric_price = price[this_month]['electric_price']
    water_price = price[this_month]['water_price']

    data = query('tenants')
    all_records = []
    print(data.keys())
    month_tocal = [this_month] 
    ask = input("Month to calculate [add / all]: ")
    if ask == 'all':
        month_tocal = list(data.keys())
    else:
        while ask !=  '':
            if ask in data.keys():
                month_tocal.append(ask)
            else:
                print(ask, "not in data")
            ask = input(" ")
        for i in month_tocal:
            try:
                datetime.strptime(i, "%Y%m")
            except Exception as ex:
                print(ex)
    month_tocal = list(set(month_tocal))

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
            if month in month_tocal:
                info = calculate(room, info)
                for fo in info:
                    update('tenants', f'{month}.{room}.{fo}', info[fo])
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
    with open(file_all, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    with open(file_price, "w", encoding="utf-8") as f:
        json.dump(price, f, ensure_ascii=False, indent=4)

    print("✅ Đã tạo rent_report.csv và rent_data_updated.json kèm cột Zalo link")


def view():
    from IPython.display import HTML
    url = "https://sites.google.com/view/trosupham2vietxocodien"
    return(HTML(f'<a href="{url}" target="_blank">👉 Mở trang web</a>'))

def query(table):
    import sqlite3
    import json
    from google.colab import drive
    drive.mount('/content/drive')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT * FROM {table} WHERE id = 1")
        x = json.loads(cursor.fetchone()[1])
    except Exception as ex:
        print(ex)
    finally:
        conn.close()
    return(x)

# =============================================================================
# ('prices', '202507.electric_price', 3000")
# =============================================================================
def update(table, object_address, value_update):
    import sqlite3
    from google.colab import drive
    drive.mount('/content/drive')
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    try:
        cursor.execute(f"""
                    UPDATE {table}
                    SET data = json_set(data, '$.{object_address}', ?)
                    WHERE id = 1
                    """, (value_update,)
            )
        conn.commit()
    except Exception as ex:
        print(ex)
    finally:
        conn.close()
    
def import_json():
    import json
    import sqlite3
    from google.colab import drive
    drive.mount('/content/drive')
    file_all = "/content/drive/MyDrive/Dau_tu/data/all.json"
    file_price = "/content/drive/MyDrive/Dau_tu/data/price.json"
    with open(file_price) as file:
        price = json.loads(file.read())

    with open(file_all, "r") as f:
            tenant = json.loads(f.read())
    conn = sqlite3.connect("/content/drive/MyDrive/Dau_tu/data/inn.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tenants (
        id INTEGER PRIMARY KEY,
        data JSON
    )
    """)
    cursor.execute("INSERT INTO tenants (data) VALUES (?)", (json.dumps(tenant),))
    conn.commit()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY,
        data JSON
    )
    """)
    cursor.execute("INSERT INTO prices (data) VALUES (?)", (json.dumps(price),))
    conn.commit()
    conn.close()
