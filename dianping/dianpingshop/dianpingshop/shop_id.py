from flask import request, url_for
from flask_api import FlaskAPI, status, exceptions
import time

app = FlaskAPI(__name__)


notes = {
    0: 'do the shopping',
    1: 'build the codez',
    2: 'paint the door',
}

def note_repr(key):
    return {
        'url': request.host_url.rstrip('/') + url_for('notes_detail', key=key),
        'text': notes[key]
    }


@app.route("/", methods=['GET', 'POST'])
def notes_list():
    """
    List or create notes.
    """
    if request.method == 'POST':
        note = str(request.data.get('text', ''))
        idx = max(notes.keys()) + 1
        notes[idx] = note
        return note_repr(idx), status.HTTP_201_CREATED

    # request.method == 'GET'
    return [note_repr(idx) for idx in sorted(notes.keys())]


# @app.route("/<int:key>/", methods=['GET', 'PUT', 'DELETE'])
# def notes_detail(key):
#     """
#     Retrieve, update or delete note instances.
#     """
#     if request.method == 'PUT':
#         note = str(request.data.get('text', ''))
#         notes[key] = note
#         return note_repr(key)
#
#     elif request.method == 'DELETE':
#         notes.pop(key, None)
#         return '', status.HTTP_204_NO_CONTENT
#
#     # request.method == 'GET'
#     if key not in notes:
#         raise exceptions.NotFound()
#     return note_repr(key)

from selenium import webdriver

driver = webdriver.Chrome()
driver.get("http://www.dianping.com")
time.sleep(5)


def selenium_tt(shop_id):
    # return driver.execute_script(
    #     'return Rohr_Opt.reload("http://t.dianping.com/ajax/dealGroupShopDetail?dealGroupId=22309627&cityId=2&action=shops&page=2&regionId=0");')
    return driver.execute_script(
        'return Rohr_Opt.reload("http://www.dianping.com/shop/%s");' % shop_id)


@app.route("/<string:shop_id>/", methods=['GET', 'POST'])
def notes_detail(shop_id):
    """
    Retrieve, update or delete note instances.
    """
    if request.method == 'GET':

        return selenium_tt(shop_id)


if __name__ == "__main__":
    app.run(debug=True,port=52525,host='0.0.0.0')