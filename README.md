# hbase-inbox
Simple Flask based API service showing usage of hbase-rest-py package. 

### Prerequisites
Python >= 3.6

Running HBase REST server on http://localhost:8080

### Setup and testing

```
$ git clone git@github.com:samirMoP/hbase-inbox.git
$ cd hbase-inbox
$ python3 -m venv v-env
$ source v-env/bin/activate
(v-env) $ python3 -m pip install -r requirements.txt
(v-env) $ flask init_db
(v-env) $ flask run
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)


* Create inbox user

curl --location --request POST 'http://127.0.0.1:5000/user' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json' \
--data-raw '{
    "name": "John Doe" , 
    "email": "john@grr.la"
}'

* Get inbox user

curl --location --request GET 'http://127.0.0.1:5000/user/<user_id>'

* Create message

curl --location --request POST 'http://127.0.0.1:5000/user/<user_id>/messages' \
--header 'Accept: application/json' \
--header 'Content-Type: application/json' \
--data-raw '{
    "subject": "[Confluence] Tech > Some subject",
    "body": "Goal of this document is to explain ...",
    "sender_email": "john@example.com",
    "sender_name": "John Doe"
}'

* Get user messages

curl --location --request GET 'http://127.0.0.1:5000/user/<user_id>/messages' \
--header 'Content-Type: application/json' \
--header 'Accept: application/json'

* Search user messages by content

curl --location --request GET 'http://127.0.0.1:5000/user/<user_id>/messages?search=apple'

* Get user messaage by id

curl --location --request GET 'http://127.0.0.1:5000/user/<user_id>/messages?message_id=<message_id>'



```

 
