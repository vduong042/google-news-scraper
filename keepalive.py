from flask import Flask
from threading import Thread

app = Flask('')


@app.route('/')
def home():
  return "Alive and Running"


def run():
  app.run(host='0.0.0.0', port=5000)


def keepalive():
  t = Thread(target=run)
  t.start()
