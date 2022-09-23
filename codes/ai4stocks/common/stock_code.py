class StockCode:
    @staticmethod
    def Code62Code8(code: str):
        if code[0] == "6":
            code = "sh" + code
        elif code[0] == "0":
            code = "sz" + code
        elif code[0] == "3":
            code = "sz" + code
        return code
