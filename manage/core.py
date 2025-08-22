db_file = "/content/drive/MyDrive/Dau_tu/data/inn.db"
file_price = "/content/drive/MyDrive/Dau_tu/data/prices.json"
file_room = "/content/drive/MyDrive/Dau_tu/data/rooms.json"
file_tenant = "/content/drive/MyDrive/Dau_tu/data/tenants.json"
file_report = "/content/drive/MyDrive/Dau_tu/report/rent_report.xlsx"   

# =============================================================================
# mount drive folder
# =============================================================================
def safe_mount_drive(mount_point="/content/drive"):
    import os
    import io
    import contextlib
    from google.colab import drive
    if not os.path.ismount(mount_point):
        f = io.StringIO()
        with contextlib.redirect_stdout(f):
            drive.mount(mount_point)
            
# =============================================================================
# calculate and gen report file
# =============================================================================
def run(*month_input):
    import json
    import pandas as pd
    from datetime import datetime

    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")

    safe_mount_drive()

    price = query('prices')
    electric_price = price[this_month]['electric_price']
    water_price = price[this_month]['water_price']

    data = query('rooms')
    all_records = []
    print(data.keys())
    month_tocal = [this_month] 
    if len(month_input) == 0:
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
            if month in month_tocal and info['status'] == 'rented':
                info = calculate(room, info)
                for fo in ['electric_fee','water_fee','rent_price','payment','bill','due_amount', 'status']:
                    update('rooms', f'{month}.{room}.{fo}', info[fo])
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
                "zalo_link": f"https://zalo.me/{info['phone']}" if info.get("phone") else "",
                "call": f'''href="tel:0886320720"{info['phone']}''' if info.get("phone") else ""
            })

    df = pd.DataFrame(all_records)
    import pandas as pd
    from openpyxl import load_workbook
    from openpyxl.utils import get_column_letter
    
    # Xuất dữ liệu ra Excel bằng pandas + openpyxl
    df = pd.DataFrame(all_records)
    df.to_excel(file_report, index=False, sheet_name="Report", engine="openpyxl")
    # Mở lại file bằng openpyxl để chỉnh sửa
    wb = load_workbook(file_report)
    ws = wb["Report"]
    # Auto-fit độ rộng cột
    for col in ws.columns:
        max_len = 0
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_len + 2
    # Định dạng tất cả cột số thành có phân cách hàng nghìn
    for col in ws.iter_cols(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in col:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0"
    # Cột cuối zalo_link thành hyperlink
    last_col = ws.max_column
    for row in range(2, ws.max_row + 1):
        url = ws.cell(row=row, column=last_col).value
        if url and str(url).startswith("http"):
            ws.cell(row=row, column=last_col).hyperlink = url
            ws.cell(row=row, column=last_col).value = "Zalo Link"   # hoặc giữ nguyên url nếu muốn
            ws.cell(row=row, column=last_col).style = "Hyperlink"
    # Lưu lại
    wb.save(file_report)
    
    with open(file_room, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    with open(file_price, "w", encoding="utf-8") as f:
        json.dump(price, f, ensure_ascii=False, indent=4)
    # with open(file_tenant, "w", encoding="utf-8") as f:
    #     json.dump(tenant, f, ensure_ascii=False, indent=4)

    print("✅ created rent_report.xlsx,room.json,price.json")

# =============================================================================
# gen link webpage
# =============================================================================
def view():
    from IPython.display import HTML
    url = "https://sites.google.com/view/trosupham2vietxocodien"
    return(HTML(f'<a href="{url}" target="_blank">👉 Mở trang web</a>'))

def query(table):
    import sqlite3
    import json
    safe_mount_drive()
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
# ('prices', '202507.R3.electric_price', 3000/"abc")
# =============================================================================
def update(table, object_address, value_update):
    import sqlite3
    safe_mount_drive()
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

# =============================================================================
# creating db file by import direct from json: price, room, tenant
# =============================================================================
def import_json():
    import json
    import sqlite3
    from google.colab import drive
    safe_mount_drive()
    with open(file_price) as file:
        price = json.loads(file.read())
    with open(file_room, "r") as f:
            room = json.loads(f.read())
    with open(file_tenant, "r") as f:
            tenant = json.loads(f.read())

    conn = sqlite3.connect("/content/drive/MyDrive/Dau_tu/data/inn.db")
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS rooms (
        id INTEGER PRIMARY KEY,
        data JSON
    )
    """)
    cursor.execute("INSERT INTO rooms (data) VALUES (?)", (json.dumps(room),))
    conn.commit()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        id INTEGER PRIMARY KEY,
        data JSON
    )
    """)
    cursor.execute("INSERT INTO prices (data) VALUES (?)", (json.dumps(price),))
    conn.commit()

    # cursor.execute("""
    # CREATE TABLE IF NOT EXISTS tenants (
    #     id INTEGER PRIMARY KEY,
    #     data JSON
    # )
    # """)
    # cursor.execute("INSERT INTO tenants (data) VALUES (?)", (json.dumps(tenant),))
    # conn.commit()
    conn.close()

# =============================================================================
# insert water and electricity consumed
# =============================================================================
def dien_nuoc():
    from datetime import datetime
    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")
    mes_elec = f"Công tơ ĐIỆN tháng {this_month}: "
    mes_water = f"Công tơ NƯỚC tháng {this_month}: "
    room = input("Room: ").upper()
    rooms = query('rooms')[this_month]
    if room not in rooms:
        print("Room not valid")
        return
    elif rooms[room]['status'] != 'rented':
        print("Room not rented yet")
        return
    elec_end = int(input(mes_elec))
    water_end = int(input(mes_water))
    elec_end = elec_end if elec_end > rooms[room]['electric_start'] else rooms[room]['electric_start']
    water_end = water_end if water_end > rooms[room]['water_end'] else rooms[room]['water_end']
    update('rooms', f'{this_month}.{room}.electric_end', elec_end)
    update('rooms', f'{this_month}.{room}.water_end', water_end)
    print("done")

# =============================================================================
# insert customer payment
# =============================================================================
def pay():
    from datetime import datetime
    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")
    room = input("Room: ").upper()
    rooms = query('rooms')[this_month]
    if room not in rooms:
        print("Room not valid")
        return
    elif rooms[room]['status'] != 'rented':
        print("Room not rented yet")
        return
    paid = rooms[room]['payment']
    if paid != 0:
        message = f"{room} already paid: {paid:,.0f}\n[y] to continue: "
        ask = input(message)
        if ask.upper() != "Y":
            return
    payment = paid + int(input("Payment: ")) 
    update('rooms', f'{this_month}.{room}.payment', payment)
    update('rooms', f'{this_month}.{room}.payment_date', datetime.strftime(today, "%d/%m/%Y"))
    print(f"{room} marked paid {payment:,.0f} at {datetime.strftime(today, "%d/%m/%Y")}")
    run(1) # (1) to avoid asking month

# =============================================================================
# add new room by insert data clob
# =============================================================================
def add_room(room_data, record_id=1):
    import json
    import sqlite3
    from datetime import datetime

    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")
    
    month = input("Month: ")
    month = month if month != '' else this_month
    room_name = input("Room: ")
    if room_name not in ["R1", "R2", "R3", "R4", "R5", "R11", "R22", "R33", "R44", "R55"]:
        print(room_name, 'not in ["R1", "R2", "R3", "R4", "R5", "R11", "R22", "R33", "R44", "R55"]')
        return
    
    safe_mount_drive()
    conn = sqlite3.connect(db_file)
    sql = f"""
        UPDATE rooms
        SET data = json_set(data, '$.{month}.{room_name}', json(?))
        WHERE id = ?
    """
    conn.execute(sql, (json.dumps(room_data), record_id))
    conn.commit()
    conn.close()

# =============================================================================
# create inform every new month from previous one    
# =============================================================================
def new_month():
    import copy
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    data = query("rooms")
    last_month = max(data.keys())   
    new_month = datetime.strftime(datetime.now(), "%Y%m")
    
    if last_month ==  new_month:
        print(new_month, "exists")
        return
    data[new_month] = copy.deepcopy(data[last_month])
    
    for room, info in data[new_month].items():
        if room in ["R1", "R2", "R3", "R4", "R5"]:
            info["start_date"] = datetime.strftime(datetime.strptime(info["start_date"], "%d/%m/%Y") + relativedelta(months=1) , "%d/%m/%Y") if info["start_date"] is not None else None
            info["end_date"] = datetime.strftime(datetime.strptime(info["end_date"], "%d/%m/%Y") + relativedelta(months=1),"%d/%m/%Y") if info["end_date"] is not None else None
            info["due_date"]   = datetime.strftime(datetime.strptime(info["due_date"], "%d/%m/%Y") + relativedelta(months=1),"%d/%m/%Y") if info["due_date"] is not None else None
            info["electric_start"] = info["electric_end"]  
            info["electric_end"]   = None  
            info["electric_fee"]   = None
            info["water_start"]    = info["water_end"]  
            info["water_end"] = None  
            info["water_fee"] = None
            info["bill"]      = None
            info["payment"]      = None if info['bill'] is None and info['payment'] is None else info['payment'] - info['bill']
            info["payment_date"] = None if info["payment"] is None else info["payment_date"]
            info["due_amount"]   = None
    safe_mount_drive()
    import json
    import sqlite3
    conn = sqlite3.connect(db_file)
    sql = """
        UPDATE rooms
        SET data = json_set(data, '$.' || ?, json(?))
        WHERE id = ?
    """
    conn.execute(sql, (new_month, json.dumps(data[new_month]), 1))
    conn.commit()
    conn.close()
    print(new_month, "initialized. Reset any room: ")
    room_reset = input().upper()
    if room_reset == '':
        return
    if room_reset not in ["R1", "R2", "R3", "R4", "R5"]:
        print(room_reset, 'not in ["R1", "R2", "R3", "R4", "R5"]')
        return
    reset_room(room_reset) 
    
# =============================================================================
# reset info of room for fresh
# =============================================================================
def reset_room(*room_reset):
    if len(room_reset) == 0:
        room = input("Room to reset: ").upper()
    else: 
        room = room_reset[0]
    if room not in ["R1", "R2", "R3", "R4", "R5", "R11", "R22", "R33", "R44", "R55"]:
        print(room, 'not in ["R1", "R2", "R3", "R4", "R5", "R11", "R22", "R33", "R44", "R55"]')
        return
    from datetime import datetime
    this_month = datetime.strftime(datetime.now(), "%Y%m")
    data = query("rooms")[this_month][room]
    for info in data:
        if info == 'status':
            data[info] = 'available'
        elif info == 'electric_start':
            data[info] = data[info] if data['electric_end'] is None else data['electric_end']
        elif info == 'water_start':
            data[info] = data[info] if data['water_end'] is None else data['water_end']
        else:
            data[info] = None
    safe_mount_drive()
    import sqlite3
    import json
    conn = sqlite3.connect(db_file)
    sql = f"""
        UPDATE rooms
        SET data = json_set(data, '$.{this_month}.{room}', json(?))
        WHERE id = ?
    """
    conn.execute(sql, (json.dumps(data), 1))
    conn.commit()
    conn.close()
    print(room, "already reset")
    
