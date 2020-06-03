


import os
import subprocess
import datetime
from flask import Flask, request
from timeit import default_timer

HOME_DIRECTORY = os.path.dirname(os.path.abspath(__file__)) #"/home/mktg_analytics/"
TRAINING_CLEANING = "download/test1.sh"
PORT = 5390


def make_bash_command(directory, script):
    full_path = os.path.join(directory, script)
    return "bash {0}".format(full_path)

def run_bash_command(cmd):
    t0 = default_timer()
    process = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    process.wait()
    t1 = default_timer()

    current_time = str(datetime.datetime.now())
    return "Time : {0}, Time elapsed (s) : {0}".format(current_time, t1 - t0)


app = Flask(__name__)


@app.route('/')
def index():
    msg = []
    msg.append("Customer value model")
    msg.append("Port: {0}".format(PORT))
    msg.append("<br>To run the scripts listed below, make an empty POST request to the corresponding endpoint.<br>")
    msg += ["/model", os.path.join(HOME_DIRECTORY, TRAINING_CLEANING), "<br>",
            "/", os.path.join(HOME_DIRECTORY, TRAINING_CLEANING)]
    return "<br>".join(msg)


@app.route('/test', methods=['POST', 'GET'])
def test():
    return run_bash_command("sudo rm {0}".format(os.path.join(HOME_DIRECTORY, "testfile")))


def request_handler(TRAINING_CLEANING):
    cmd = make_bash_command(HOME_DIRECTORY, TRAINING_CLEANING)
    if request.method == "POST":
        res = run_bash_command(cmd)
        return "Prediction complete. {0}".format(res)
    elif request.method == "GET":
        return cmd


@app.route('/model', methods=['POST', 'GET'])
def model():
    return request_handler(TRAINING_CLEANING)


#@app.route('/model_fgt', methods=['POST', 'GET'])
#def model():
#    return request_handler(CREATE_TRAINING_SAMPLE)

#
#@app.route('/model_app', methods=['POST', 'GET'])
#def model():
#    return request_handler(CREATE_MODEL)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
