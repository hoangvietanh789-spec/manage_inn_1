db_file = "/content/drive/MyDrive/Dau_tu/data/inn.db"
file_price = "/content/drive/MyDrive/Dau_tu/data/prices.json"
file_room = "/content/drive/MyDrive/Dau_tu/data/rooms.json"
file_tenant = "/content/drive/MyDrive/Dau_tu/data/tenants.json"
file_report = "/content/drive/MyDrive/Dau_tu/report/rent_report.xlsx"  
file_cashflow =  "/content/drive/MyDrive/Dau_tu/report/cash_flow.xlsx"  

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
    tong_diennuoc = querydf('tong_diennuoc')
    if this_month not in tong_diennuoc['Month']:
        print("Chưa có hóa đơn tổng điện nước tháng này")
        return

    tenant = query('tenants')    

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
        prepayment = info['prepayment'] or 0

        bill = rent_price + electric_fee + water_fee
        if bill <= prepayment:
            bill = 0
            prepayment = prepayment - bill
            due_amount = 0
        elif bill > prepayment:
            bill = due_amount = bill - prepayment
            prepayment = 0

        # Cập nhật vào dict
        info["electric_fee"] = electric_fee
        info["water_fee"] = water_fee
        info["bill"] = bill
        info["due_amount"] = due_amount
        info["prepayment"] = prepayment

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
                    update('rooms', f'{month}.{room}.{fo}', info[fo]) #update trước rồi mới convert để gen file như dưới đây
            info['bill'] = 0 if info['bill'] == None else info['bill']
            info['rent_price'] = 0 if info['rent_price'] == None else info['rent_price']
            info['electric_fee'] = 0 if info['electric_fee'] == None else info['electric_fee']
            info['electric_end'] = 0 if info['electric_end'] == None else info['electric_end']
            info['electric_start'] = 0 if info['electric_start'] == None else info['electric_start']
            info['water_fee'] = 0 if info['water_fee'] == None else info['water_fee']
            info['water_end'] = 0 if info['water_end'] == None else info['water_end']
            info['water_start'] = 0 if info['water_start'] == None else info['water_start']
            info['payment'] = 0 if info['payment'] == None else info['payment']
            info['prepayment'] = 0 if info['prepayment'] == None else info['prepayment']

            all_records.append({
                "month": month,
                "room": room,
                "tenant": info["phone"],
                "name": info["name"],
                "rent_price": info["rent_price"],
                "electric_fee": info["electric_fee"],
                "water_fee": info["water_fee"],
                "bill": info["bill"],
                "prepayment": info["prepayment"],
                "payment": info["payment"],
                "due_amount": info["due_amount"],
                "status": info["status"],
                "zalo_link": f"https://zalo.me/{info['phone']}" if info.get("phone") else "",
                "notify":f"""Tổng: {info["bill"]:,.0f}, trong đó:
    - Tiền thuê: {info["rent_price"]:,.0f}
    - Tiền điện: {info["electric_fee"]:,.0f} = ({info["electric_end"]:,.0f} - {info["electric_start"]:,.0f}) * {electric_price:,.0f}đ/kWh
    - Tiền nước: {info["water_fee"]:,.0f} = ({info["water_end"]:,.0f} - {info["water_start"]:,.0f}) * {water_price:,.0f}đ/m3
    - Đã thanh toán/trả trước: {info["payment"]:,.0f}
    - Còn thiếu: {info["due_amount"]:,.0f}"""
            })
            
    import pandas as pd
    from openpyxl import load_workbook
    from openpyxl.utils import get_column_letter
    
    # Xuất dữ liệu ra Excel bằng pandas + openpyxl
    df = pd.DataFrame(all_records)
    df['notify'] = df.apply(lambda row: row['notify'] if row['due_amount'] not in [0,'0'] else '', axis = 1) # bỏ notify các dòng đẫ thanh toán
    df = df[df['status'] != 'available'] # bỏ bớt các dòng chưa cho thuê
    col_remain = list(df.columns) 
    col_remain.remove('status')
    df = df[col_remain]
    df.to_excel(file_report, index=False, sheet_name="Report", engine="openpyxl")
    # Mở lại file bằng openpyxl để chỉnh sửa
    wb = load_workbook(file_report)
    ws = wb["Report"]
    # Auto-fit độ rộng cột
    for col in ws.columns:
        max_len = 0
        min_len = 55
        col_letter = get_column_letter(col[0].column)
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        ws.column_dimensions[col_letter].width = max_len + 2 if max_len < min_len else min_len # để cột cuối ko bị quá dài
    # Định dạng tất cả cột số thành có phân cách hàng nghìn
    for col in ws.iter_cols(min_row=2, max_row=ws.max_row, min_col=1, max_col=ws.max_column):
        for cell in col:
            if isinstance(cell.value, (int, float)):
                cell.number_format = "#,##0"

    zalo_col = None
    name_col = None
    room_col = None
    for col in range(1, ws.max_column + 1):
        if ws.cell(row=1, column=col).value == "zalo_link":
            zalo_col = col
        elif ws.cell(row=1, column=col).value == "name":
            name_col = col
        elif ws.cell(row=1, column=col).value == "room":
            room_col = col
    for row in range(2, ws.max_row + 1):
        url = ws.cell(row=row, column=zalo_col).value
        name = ws.cell(row=row, column=name_col).value
        room = ws.cell(row=row, column=room_col).value
        if url and str(url).startswith("http"):
            ws.cell(row=row, column=zalo_col).hyperlink = url
            ws.cell(row=row, column=zalo_col).value = f"Zalo {room} {name}"
            ws.cell(row=row, column=zalo_col).style = "Hyperlink"
    wb.save(file_report)
    
    with open(file_room, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    with open(file_price, "w", encoding="utf-8") as f:
        json.dump(price, f, ensure_ascii=False, indent=4)
    with open(file_tenant, "w", encoding="utf-8") as f:
        json.dump(tenant, f, ensure_ascii=False, indent=4)

    print("✅ created rent_report.xlsx,room.json,price.json,tenant.json")

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

def querydf(table):
    import pandas as pd
    import sqlite3
    conn = sqlite3.connect(db_file)
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn)
    conn.close()
    return df

# =============================================================================
# ('prices', '202507.R3.electric_price', 3000/"abc")
# - cập nhật thông tin các bảng theo đường dẫn
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

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tenants (
        id INTEGER PRIMARY KEY,
        data JSON
    )
    """)
    cursor.execute("INSERT INTO tenants (data) VALUES (?)", (json.dumps(tenant),))
    conn.commit()
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
    elec_end = elec_end if (rooms[room]['electric_start'] is None or elec_end > rooms[room]['electric_start']) else rooms[room]['electric_start']
    water_end = water_end if (rooms[room]['water_end'] is None or water_end > rooms[room]['water_end']) else rooms[room]['water_end']
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
def add_room(room_data):
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
    conn.execute(sql, (json.dumps(room_data), 1))
    conn.commit()
    conn.close()
    
# =============================================================================
# add new room by insert data clob
# =============================================================================
def add_tenant(tenant_data):
    import json
    import sqlite3
    tenants = query('tenants')
    if tenant_data['phone'] != tenant_data['zalo']:
        key = input("chose phone or zalo because diff: 1/2")
        if key not in ['1', '2']:
            print("bad option")
            return
        key = tenant_data['phone'] if key == '1' else tenant_data['zalo']
    else:
        key = tenant_data['phone']
    if key in tenants:
        print(f"{key} already in tenants")
        return
    safe_mount_drive()
    conn = sqlite3.connect(db_file)
    sql = f"""
        UPDATE tenants
        SET data = json_set(data, '$.active.{key}', json(?))
        WHERE id = ?
    """
    conn.execute(sql, (json.dumps(tenant_data), 1))
    conn.commit()
    conn.close()
    print(key, 'added')
    print(query('tenants')[key])

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
            info["payment"] = None 
            info["payment_date"] = None 
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
    automap_tenant() # tự động rà soát cập nhật lại thông tin người thuê, chỉ lấy thông tin người thuê đang active. lấy đúng room người thuê
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
        elif info == 'num':
            data[info] = 0
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
    
# =============================================================================
# automatically map tenants info to rooms, only this month
# Tự động cập nhật, chỉ áp dụng cho tháng này
# Kiểm tra đối chiếu ngày bắt đầu thuê của từng tenant
# Tự động đếm và cập nhật thông tin số người thuê cho từng phòng
# Chỉ lấy thông tin của người thuê là main hoặc người thuê là main cuối cùng nếu cả 2 cùng là main
# =============================================================================
def automap_tenant():
    from datetime import datetime
    this_month = datetime.strftime(datetime.now(), "%Y%m")
    rooms = query("rooms")[this_month]
    tenants = query("tenants")['active']
    for r in rooms:
        num = 0
        for t in tenants:
            tenant = tenants[t]
            tenant_start = datetime.strftime(datetime.strptime(tenant['start_date'], '%d/%m/%Y'), "%Y%m")
            if tenant['room'] == r and tenant['main'] == 1 and tenant_start <= this_month:
                update('rooms', f'{this_month}.{r}.rent_price', tenant['rent_price'])
                update('rooms', f'{this_month}.{r}.deposit', tenant['deposit'])
                update('rooms', f'{this_month}.{r}.deposit_date', tenant['deposit_date'])
                update('rooms', f'{this_month}.{r}.phone', tenant['phone'])
                update('rooms', f'{this_month}.{r}.zalo', tenant['zalo'])
                update('rooms', f'{this_month}.{r}.name', tenant['name'])
            else: 
                print(f"check room: {tenant['room']} == {r}: {tenant['room'] == r}")
                print(f"check main: {tenant['main']} == 1: {tenant['main'] == 1}")
                print(f"check month: {tenant_start} <= {this_month}: {tenant_start <= this_month}")
            if tenant['room'] == r:
                num += 1
                update('rooms', f'{this_month}.{r}.num', num)
                
# =============================================================================
# manual map tenant info to room, only this month           
# cập nhật thông tin người thuê vào phòng bằng cạch chọn người thuê và phòng cần cập nhật
# Người thuê chỉ được cập nhật vào đúng phòng thuê đã khai báo trong tenant và main phải =1  
# =============================================================================
def manualmap_tenant():
    from datetime import datetime
    this_month = datetime.strftime(datetime.now(), "%Y%m")
    rooms = query("rooms")[this_month]
    tenants = query("tenants")['active']
    print(tenants.keys())
    t = input("tenant: ")
    if t not in tenants:
        print(t, "not in tenants")
        return
    tenant = tenants[t]
    print(rooms.keys())
    r = input("room: ").upper()
    if r not in rooms:
        print(r, "not in rooms")
        return
    num = 0
    tenant_start = datetime.strftime(datetime.strptime(tenant['start_date'], '%d/%m/%Y'), "%Y%m")
    if tenant['room'] == r and tenant['main'] == 1 and tenant_start <= this_month:
        update('rooms', f'{this_month}.{r}.rent_price', tenant['rent_price'])
        update('rooms', f'{this_month}.{r}.deposit', tenant['deposit'])
        update('rooms', f'{this_month}.{r}.deposit_date', tenant['deposit_date'])
        update('rooms', f'{this_month}.{r}.phone', tenant['phone'])
        update('rooms', f'{this_month}.{r}.zalo', tenant['zalo'])
        update('rooms', f'{this_month}.{r}.name', tenant['name'])
    else:
        print(f"check room: {tenant['room']} == {r}: {tenant['room'] == r}")
        print(f"check main: {tenant['main']} == 1: {tenant['main'] == 1}")
        print(f"check month: {tenant_start} <= {this_month}: {tenant_start <= this_month}")
                          
    if tenant['room'] == r:
        num+= 1
        update('rooms', f'{this_month}.{r}.num', num)         
        
# =============================================================================
# change tenant status: tenant = "sdt", status = "active/deactive" 
# - cập nhật theo input
# - nếu là active thì tự động xóa ngày end_date là ngày chấm dứt thuê, cập nhật ngày start_date là ngày thuê theo input
# - nếu là deactive thì tự động cập nhật ngày end_date là ngày chấm dứt thuê, ngày start_date giữ nguyên
# =============================================================================
def change_tenant_status(**kwargs):
    from datetime import datetime
    def status_tenant(new_data):
        import sqlite3
        import json
        safe_mount_drive()
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        cursor.execute("""
                       UPDATE tenants
                       SET data = ?
                       WHERE id = 1
                       """, (json.dumps(new_data),))
        conn.commit()
        print("updated")
        conn.close()
    tenants = query("tenants")
    active = tenants['active']
    deactive = tenants['deactive']
    if kwargs['tenant'] not in active and kwargs['tenant'] not in deactive:
        print(kwargs['tenant'], "not exists")
        return
    if kwargs['status'] == 'active':
        if kwargs['tenant'] in active:
            print('already active')
            return
        else:
            active[kwargs['tenant']] = active.get(kwargs['tenant'], deactive[kwargs['tenant']])
            start_date = input("start_date (dd/mm/yyyy): ")
            room = input("room: ").upper()
            rent_price = int(input("rent_price: "))
            mess_deposit = f"deposit / enter = {rent_price}: "
            deposit = input(mess_deposit)
            deposit = int(deposit) if deposit != '' else rent_price
            deposit_date = input("deposit_date (dd/mm/yyyy): ")
            try:
                datetime.strptime(start_date, '%d/%m/%Y')
                datetime.strptime(deposit_date, '%d/%m/%Y')
                active[kwargs['tenant']]['start_date'] = start_date
                active[kwargs['tenant']]['end_date'] = None
                active[kwargs['tenant']]['rent_price'] = rent_price
                active[kwargs['tenant']]['deposit'] = deposit
                active[kwargs['tenant']]['deposit_date'] = deposit_date
                active[kwargs['tenant']]['room'] = room
            except Exception as ex:
                print(ex)
                return
            deactive.pop(kwargs['tenant'])
            tenants['active'],tenants['deactive'] =  active, deactive
            status_tenant(tenants)
            print('activated')
    elif kwargs['status'] == 'deactive':
        if kwargs['tenant'] in deactive:
            print('already deactive')
            return
        else:
            deactive[kwargs['tenant']] = deactive.get(kwargs['tenant'], active[kwargs['tenant']])
            end_date = input("end_date (dd/mm/yyyy): ")
            try:
                datetime.strptime(end_date, '%d/%m/%Y')
                deactive[kwargs['tenant']]['end_date'] = end_date
            except Exception as ex:
                print(ex)
                return
            active.pop(kwargs['tenant'])
            tenants['active'],tenants['deactive'] =  active, deactive
            status_tenant(tenants)
            print('deactivated')
            message = f"need to reset room {active[kwargs['tenant']]['Room']} [yes]: "
            ask = input(message).upper()
            if ask == "YES":
                reset_room(active[kwargs['tenant']]['Room'])
    else:
        print("wrong status")

def doanhthu():
    import pandas as pd
    import sqlite3
    from openpyxl import load_workbook
    from openpyxl.utils import get_column_letter
    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    rooms = query('rooms')
    tenants = query('tenants')
    thu = []

    # --- Thu tiền phòng ---
    for month in rooms:
        for room in rooms[month]:
            if rooms[month][room]['payment'] and rooms[month][room]['payment'] > 0:
                thu.append({
                    "Ngày": rooms[month][room]['payment_date'],
                    "Số tiền": rooms[month][room]['payment'],
                    "Nội dung": f"Phòng {room}, tháng {month}",
                    "Tiền điện": rooms[month][room]['electric_fee'],
                    "Tiền nước": rooms[month][room]['water_fee'],
                    "Month": month
                })

    # --- Thu tiền cọc ---
    for status in tenants:
        for tenant in tenants[status]:
            if tenants[status][tenant]['deposit'] != 0:
                thu.append({
                    "Ngày": tenants[status][tenant]['deposit_date'],
                    "Số tiền": tenants[status][tenant]['deposit'],
                    "Nội dung": f"Phòng {tenants[status][tenant]['room']} đặt cọc",
                    "Tiền điện": None,
                    "Tiền nước": None,
                    "Month": None
                })

    # --- Tạo DataFrame ---
    df = pd.DataFrame(thu)
    df = df[df['Số tiền'] != 0]

    # Thêm cột tháng điện/nước (lùi 1 tháng)
    df['month_water_el'] = df['Month'].apply(
        lambda x: datetime.strftime(datetime.strptime(str(x), "%Y%m") - relativedelta(months=1), "%Y%m")
                  if pd.notna(x) else x
    )

    # Sắp xếp theo ngày
    df['Ngày_dt'] = pd.to_datetime(df['Ngày'], format="%d/%m/%Y", errors="coerce")
    df = df.sort_values(by="Ngày_dt", ascending=False).drop(columns=["Ngày_dt"])

    # --- Xuất Excel ---
    df.to_excel(file_cashflow, index=False, sheet_name="Report", engine="openpyxl")

    wb = load_workbook(file_cashflow)
    ws = wb["Report"]

    # Auto-fit độ rộng cột
    for col in ws.columns:
        max_len = 0
        for cell in col:
            if cell.value:
                max_len = max(max_len, len(str(cell.value)))
        width = min(max_len + 2, 55)
        ws.column_dimensions[get_column_letter(col[0].column)].width = width

    # Định dạng số cho các cột tiền
    for col_name in ["Số tiền", "Tiền điện", "Tiền nước"]:
        if col_name in df.columns:
            col_idx = df.columns.get_loc(col_name) + 1
            for cell in ws.iter_cols(min_row=2, max_row=ws.max_row, min_col=col_idx, max_col=col_idx):
                for c in cell:
                    if isinstance(c.value, (int, float)):
                        c.number_format = "#,##0"

    wb.save(file_cashflow)

    # --- Ghi đè bảng cashflow trong SQLite ---
    conn = sqlite3.connect(db_file)
    df.to_sql("cashflow", conn, if_exists="replace", index=False)
    conn.close()


# import sqlite3

# conn = sqlite3.connect(db_file)
# cursor = conn.cursor()

# cursor.execute("""
# CREATE TABLE IF NOT EXISTS tong_diennuoc (
#     Month TEXT PRIMARY KEY,
#     So_dien REAL,
#     So_nuoc REAL,
#     Tien_dien REAL,
#     Tien_nuoc REAL,
#     Gia_dien REAL,
#     Gia_nuoc REAL
# )
# """)
# conn.commit()
# conn.close()

def save_utility(month, so_dien, so_nuoc, tien_dien, tien_nuoc):
    from datetime import datetime

    today = datetime.now()
    this_month = datetime.strftime(today, "%Y%m")

    if month != this_month:
        print("Tháng không phải tháng hiện tại")
        ask = input("Có muốn tiếp tục không [yessss]: ").lower()
        if ask != 'yessss':
            return

    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()

    # Tính giá bình quân
    gia_dien = math.ceil(tien_dien / so_dien / 1000) * 1000 if so_dien > 0 else 0
    gia_nuoc = math.ceil(tien_nuoc / so_nuoc / 1000) * 1000 if so_nuoc > 0 else 0
    electric_price = query('prices')[month]['electric_price']
    water_price = query('prices')[month]['water_price']
    if gia_dien > electric_price:
        update('prices', f'{month}.electric_price', gia_dien)
    if gia_nuoc > water_price:
        update('prices', f'{month}.water_price', gia_nuoc)


    cursor.execute("""
        INSERT INTO tong_diennuoc (Month, So_dien, So_nuoc, Tien_dien, Tien_nuoc, Gia_dien, Gia_nuoc)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(Month) DO UPDATE SET
            So_dien=excluded.So_dien,
            So_nuoc=excluded.So_nuoc,
            Tien_dien=excluded.Tien_dien,
            Tien_nuoc=excluded.Tien_nuoc,
            Gia_dien=excluded.Gia_dien,
            Gia_nuoc=excluded.Gia_nuoc
    """, (month, so_dien, so_nuoc, tien_dien, tien_nuoc, gia_dien, gia_nuoc))
    conn.commit()
    conn.close()
    print(f"Đã lưu tháng {month}: Giá điện {gia_dien:,} đ/kWh, Giá nước {gia_nuoc:,} đ/m³")
    run(1)
    print("Đã cập nhật giá vào room")

