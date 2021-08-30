import json


class Configure:

    def __init__(self, file):
        self.file = file

    def config(self):
        with open(self.file, "r") as f:
            cred = json.load(f)

        password = cred["password"]
        user = cred["userid"]
        account = cred["account"]
        warehouse = cred["warehouse"]
        database = cred["database"]
        schema = cred["schema"]
        source = cred["filesource"]
        archive = cred["archive"]
        role = cred["role"]
        client1 = cred["client1"]
        client2 = cred["client2"]
        temp = cred["temp"]
        return {"password": password, "user": user, "account": account, "warehouse": warehouse, "database": database,
                "schema": schema, "source": source, "archive": archive, "role": role,
                "client1": client1, "client2": client2, "temp": temp}
