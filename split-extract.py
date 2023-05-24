def remove_inside_get_outside(account, config):
    open_char = config["opening_char"]
    close_char = config["closing_char"]
    start = False
    remove_str = ""
    for char in account:
        if char == open_char:
            start = True
        if start:
            if (
                char in [open_char, close_char]
                or char.isnumeric() == True
            ):
                remove_str += char
        if char == close_char:
            start = False
            break
    return account.replace(remove_str, "")

# alternative of recoil: zustand

extract_split_fields = {
        "opening_char": '-',
        "closing_char": '',
        "inside_outside": 'inside'
    }
input_data = [
    "Entertainment - 100% business (400)",
    "Advertising & Marketing (400)",
    "Audit & Accountancy fees (401)",
    "Entertainment-100% business (420)",
    "General Expenses (429)",
    "Light, Power, Heating (445)",
    "Motor Vehicle Expenses (449)",
    "Postage, Freight & Courier (425)",
    "Printing & Stationery (461)",
    "Purchases (300)",
    "Rent (469)",
    "Subscriptions (485)",
    "Telephone & Internet (489)",
    "Travel - National (493)",
    "Account Balance (700) (qabc)",
    "43500 - some text - anoter text"
]

output_data = list()

for account in input_data:
    if extract_split_fields["opening_char"] in account:
        splited_account = account.split(
            extract_split_fields["opening_char"]
        )
        if extract_split_fields["inside_outside"] == "outside":
            if "-" in account:
                for i in splited_account:
                    if i.strip().isnumeric():
                        output_data.append(i.strip())
                
            else:
                output_data.append(
                    remove_inside_get_outside(
                        account, extract_split_fields
                    )
                )
        else:
            if "-" in account:
                op=account.split("-")[-1]
                cp = 1
                flag=(op).isnumeric()
                while flag == False and cp < len(op):
                    op = op.split(extract_split_fields["opening_char"])
                    for item in op:
                        if extract_split_fields["closing_char"] in item:
                            op = item.split(extract_split_fields["closing_char"])
                            if op[0].isnumeric():
                                flag = True
                                output_data.append(op[0])
            else:
                current_pos = 1
                v = splited_account[(current_pos * -1)].replace(
                    extract_split_fields["closing_char"], ""
                )
                flag = v.isnumeric()
                while flag == False and current_pos < len(
                    splited_account
                ):
                    current_pos = current_pos + 1
                    v = splited_account[
                        (current_pos * -1)
                    ].replace(
                        extract_split_fields["closing_char"], ""
                    )
                    flag = (v.strip()).isnumeric()
                output_data.append(v)
print("output_data: ", output_data)
