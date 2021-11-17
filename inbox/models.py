import json
import sys
import time
import datetime
from hbase.rest_client import HBaseRESTClient
from hbase.admin import HBaseAdmin
from hbase.put import Put
from hbase.get import Get
from hbase.scan import Scan
from hbase.utils import Bytes
from hbase.scan_filter_helper import (
    build_column_prefix_filter,
    build_value_filter,
    build_base_scanner,
)

"""
HBase scheme design example for storing user message
"""


# Modes BaseModel, User, Message


class BaseModel(object):
    def __init__(self, data):

        self.defaults = {"_raw_json": None}
        setattr(self, "_raw_json", data)

    def __str__(self):
        return self.asJsonString()

    def asJsonString(self):
        return json.dumps(
            self.asDict(), sort_keys=True, separators=(",", ":"), indent=4
        )

    def asDict(self):

        data = {}

        for (key, value) in self._raw_json.items():
            if isinstance(self._raw_json.get(key, None), (list, tuple, set)):
                data[key] = list()
                for subobj in self._raw_json.get(key, None):
                    data[key].append(subobj)

            elif self._raw_json.get(key, None):
                data[key] = self._raw_json.get(key, None)

        return data


class User(BaseModel):
    def __init__(self, data):
        super(User, self).__init__(data=data)

        self.defaults = {"id": None, "name": None, "email": None}

        for (param, default) in self.defaults.items():
            setattr(self, param, data.get(param, default))


class Message(BaseModel):
    def __init__(self, data):
        super(Message, self).__init__(data=data)

        self.defaults = {
            "id": None,
            "subject": None,
            "body": None,
            "sender_email": None,
            "sender_name": None,
            "send_date": None
        }

        for (param, default) in self.defaults.items():
            setattr(self, param, data.get(param, default))

def get_datetime_from_id(id):
    tmstmp_from_id = sys.maxsize - int(id)
    return datetime.datetime.fromtimestamp(int(tmstmp_from_id/1000)).strftime('%Y-%m-%d %H:%M:%S')

def generate_id():
    tmstmp = round(time.time() * 1000)
    id = sys.maxsize - tmstmp
    return id

def create_inbox_table(client):
    admin = HBaseAdmin(client=client)
    admin.table_create_or_update(
        table_name="inbox",
        params_list=[{"name": "cfu"}, {"name": "cfmm"}],
    )

def register_user(client, user):
    put = Put(client=client)
    put.put(
        tbl_name="inbox",
        row_key=str(user.id),
        cell_map={"cfu:desc": f"{user.name}:{user.email}"},
    )

def get_user(client, user_id):
    get = Get(client=client)
    result = get.get(tbl_name="inbox", row_key=str(user_id), column_family="cfu:desc")
    name_email = Bytes.to_string(result["row"][0]["cell"][0]["$"])
    name_email_lst = name_email.split(":")
    user = User(
        data={"id": user_id, "name": name_email_lst[0], "email": name_email_lst[1]}
    )
    return user.asJsonString()

def create_user_message(client, user_id, msg):
    put = Put(client=client)
    put.put(
        tbl_name="inbox",
        row_key=str(user_id),
        cell_map={
            f"cfmm:{msg.id}:{msg.subject}:{msg.sender_email}:{msg.sender_name}": msg.body,
        },
    )

def get_msgs_from_result(msg_cells):
    messages = []
    for c in msg_cells:
        value = Bytes.to_string(c["$"])
        c_list = Bytes.to_string(c["column"]).split(":")
        if c_list[0] == "cfmm":
            messages.append(
                Message(
                    data={
                        "id": c_list[1],
                        "subject": c_list[2],
                        "body": value,
                        "send_date": get_datetime_from_id(c_list[1]),
                        "sender_email": c_list[3],
                        "sender_name": c_list[4],
                    }
                )
            )
    #m_sorted = sorted(messages, key=lambda k: k.send_date, reverse=True)
    return [m.asDict() for m in messages]

def get_user_messages(client, user_id):
    get = Get(client)
    data = get.get("inbox", str(user_id))
    user_id = Bytes.to_string(data["row"][0]["key"])
    columns = data["row"][0]["cell"]
    return get_msgs_from_result(msg_cells=columns)


def search_message(client, word, user_id):
    scan = Scan(client)
    filter = build_value_filter(
        startRow=str(user_id),
        endRow=str(user_id),
        value=word,
        column=['cfmm'],
        operation="EQUAL",
        comparator="substring",
    )
    print(filter)
    _, result = scan.scan(tbl_name="inbox", scanner_payload=filter)
    columns = result["row"][0]["cell"]
    scan.delete_scanner()
    return get_msgs_from_result(columns)

def get_message_by_id(client, message_id):
    scan = Scan(client)
    filter = build_column_prefix_filter(column_prefix=str(message_id), column=['cfmm'])
    _, result = scan.scan(tbl_name="inbox", scanner_payload=filter)
    columns = result["row"][0]["cell"]
    scan.delete_scanner()
    return get_msgs_from_result(msg_cells=columns)

def scan_mesages(client, batch):
    scn = Scan(client=client)
    filter = build_base_scanner(batch=batch)
    _, result = scn.scan('inbox', filter    )
    columns = result["row"][0]["cell"]
    return get_msgs_from_result(columns)



# if __name__ == "__main__":
#
#     client = HBaseRESTClient(hosts_list=["http://localhost:8080"])
#
#     test_user = User(
#         data={"id": 245, "name": "Samir Ahmic", "email": "samir@example.com"}
#     )

    # test_message1 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "test subject",
    #         "body": "some message body",
    #         "sender_email": "test@example.com",
    #         "sender_name": "Jon Doe",
    #         "send_date": '2021-11-07'
    #     }
    # )
    # time.sleep(10)
    # test_message2 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "meeting notes",
    #         "body": "Last Monday ",
    #         "sender_email": "test2@example.com",
    #         "sender_name": "Susan Brown",
    #         "send_date": '2021-11-08'
    #     }
    # )
    # time.sleep(10)
    #
    # test_message3 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "Subscriptions execution plan",
    #         "body": "Hi, Samir. Here is plan for Apple subscriptions",
    #         "sender_email": "test3@example.com",
    #         "sender_name": "Bugs Bunny",
    #         "send_date": '2021-11-09',
    #     }
    # )
    # time.sleep(10)
    # test_message4 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "The Argentinian users case",
    #         "body": "The Argentinian bank account number field has been updated to allow for 22 digits.",
    #         "sender_email": "test4@example.com",
    #         "sender_name": "Jessica Doil",
    #         "send_date": '2021-11-10',
    #     }
    # )
    # time.sleep(10)
    #
    # test_message5 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "New DSPs, post launch",
    #         "body": "Here is new schema for post launch marketing",
    #         "sender_email": "test5@example.com",
    #         "sender_name": "Hanna Marou",
    #         "send_date": '2021-11-11',
    #     }
    # )
    #
    # time.sleep(10)
    #
    # test_message6 = Message(
    #     data={
    #         "id": generate_id(),
    #         "subject": "Using HFileLink files in split as default",
    #         "body": "Hi,I proposed an issue to use HFileLink file when splitting in",
    #         "sender_email": "test6@example.com",
    #         "sender_name": "Erik Ljunkvist",
    #         "send_date": '2021-11-12',
    #     }
    # )
    #
    # create_inbox_table(client)
    # register_user(client, test_user)
    # create_user_message(client, test_user.id, test_message1)
    # create_user_message(client, test_user.id, test_message2)
    # create_user_message(client, test_user.id, test_message3)
    # create_user_message(client, test_user.id, test_message4)
    # create_user_message(client, test_user.id, test_message5)
    # create_user_message(client, test_user.id, test_message6)


    # print("\n--------- Get user ----------")
    # print(get_user(client, test_user.id))
    # # print(get_user_messages(client, test_user.id))
    #
    #
    # print("\n----- Search message for containing word in message body------")
    # print(search_message(client, "hfile", 245))
    #
    # print("----------- All results ---------")
    #
    # print(get_user_messages(client, 245))
    #
    # print("\n--- Get 5 last messages ----")
    # print(scan_mesages(client, 5))



