import json
from flask import Flask, jsonify, request
from hbase.rest_client import HBaseRESTClient
from .models import get_user, User, generate_id, register_user, get_user_messages, search_message, \
    get_message_by_id, Message, create_user_message

app = Flask(__name__)

client = HBaseRESTClient(hosts_list=["http://localhost:8080"])

@app.route('/')
def hello():
    return 'Hello, World!'

@app.route('/user/<int:user_id>', methods=['GET'])
def get_inbox_user(user_id):
    return get_user(client, user_id)

@app.route('/user', methods=['POST'])
def create_inbox_user():
    data = json.loads(request.data)
    data['id'] = generate_id()
    user = User(data=data)
    register_user(client, user)
    return jsonify(user.asDict()) , 201

@app.route('/user/<int:user_id>/messages', methods=['GET'])
def get_user_inbox(user_id):
    params = request.args
    search_term  = params.get('search')
    message_id = params.get('message_id')
    if message_id is not None:
        data = get_message_by_id(client, message_id)
        return {'user': user_id, "messages": data}
    if search_term is not None:
        data = search_message(client, search_term, user_id)
        return {'user': user_id, "messages": data}

    data = get_user_messages(client, user_id)
    return {'user':user_id, "messages":data}

@app.route('/user/<int:user_id>/messages', methods=['POST'])
def create_message(user_id):
    data = json.loads(request.data)
    data["id"] = generate_id()
    message = Message(data=data)
    create_user_message(client, user_id, message)
    return jsonify(message.asDict()), 201





