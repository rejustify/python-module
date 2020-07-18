import os
import requests
import pandas as pd
import numpy as np
import json

# global variables
rejustify_main_url = os.environ.get('rejustify_main_url') or 'https://api.rejustify.com'
rejustify_proxy_url = os.environ.get('rejustify_proxy_url') or None
rejustify_proxy_port = os.environ.get('rejustify_proxy_port') or None
rejustify_learn = os.environ.get('rejustify_learn') or True
rejustify_token = os.environ.get('rejustify_token') or None
rejustify_email = os.environ.get('rejustify_email') or None


def setCurl(main_url=None, proxy_url=None, proxy_port=None, learn=None):
    """
    setCurl()
    """

    global rejustify_main_url, rejustify_proxy_url, rejustify_proxy_port, rejustify_learn

    # error handling
    if main_url is not None and not isinstance(main_url, str):
        raise ValueError("`main_url` parameter must be a string")
    if proxy_url is not None and not isinstance(proxy_url, str):
        raise ValueError("`proxy_url` parameter must be a string")
    if proxy_port is not None and not isinstance(proxy_port, int):
        raise ValueError("`proxy_port` parameter must be an integer")
    if learn is not None and not isinstance(learn, bool):
        raise ValueError("`learn` parameter must be True/False")

    # assign values
    if main_url is not None:
        rejustify_main_url = main_url
    if proxy_url is not None:
        rejustify_proxy_url = proxy_url
    if proxy_port is not None:
        rejustify_proxy_port = proxy_port
    if learn is not None:
        rejustify_learn = learn

    # consistency checks
    if(rejustify_proxy_url is ''):
        rejustify_proxy_url = None
    if(rejustify_proxy_port is ''):
        rejustify_proxy_port = None
    if(type(rejustify_learn) is not bool):
        rejustify_learn = True


def getCurl():
    """
    getCurl()
    """

    print('Main API address: ' + rejustify_main_url)
    print('Proxy address: ' + (rejustify_proxy_url if rejustify_proxy_url is not None else 'No proxy address'))
    print('Proxy port: ' + (rejustify_proxy_port if rejustify_proxy_port is not None else 'No proxy port'))
    print('Enable learning: ' + 'Yes' if rejustify_learn is True else 'No')


def register(token=None, email=None):
    """
    register()
    """

    global rejustify_token, rejustify_email

    # error handling
    if token is not None and not isinstance(token, str):
        raise ValueError("`token` parameter must be a string")
    if email is not None and not isinstance(email, str):
        raise ValueError("`email` parameter must be a string")

    # assign values
    if token is not None:
        rejustify_token = token
    if email is not None:
        rejustify_email = email

    # consistency checks
    if(rejustify_token is ''):
        rejustify_token = None
    if(rejustify_email is ''):
        rejustify_email = None


def analyze(df=None, shape="vertical", inits=1, fast=True,
            sep=",", learn=None, token=None,
            email=None, url=None):

    # error handling
    if df is not None and not isinstance(df, pd.DataFrame):
        raise ValueError("`df` parameter must be a DataFrame object")
    if df is None:
        raise ValueError("`df` parameter must be a DataFrame object")
    if shape is not "vertical" and shape is not "horizontal":
        raise ValueError("`shape` parameter must be vertical/horizontal")
    if inits is not None and not isinstance(inits, int):
        raise ValueError("`inits` parameter must be an integer")
    if fast is not None and not isinstance(fast, bool):
        raise ValueError("`fast` parameter must be True/False")
    if sep is not None and not isinstance(sep, str):
        raise ValueError("`sep` parameter must be a string")
    if len(sep) > 3:
        raise ValueError("`sep` has a maximum of 3 characters")
    if learn is not None and not isinstance(learn, bool):
        raise ValueError("`learn` parameter must be True/False")
    if token is not None and not isinstance(token, str):
        raise ValueError("`token` parameter must be a string")
    if email is not None and not isinstance(email, str):
        raise ValueError("`email` parameter must be a string")
    if url is not None and not isinstance(url, str):
        raise ValueError("`url` parameter must be a string")

    # set global variables
    if learn is None:
        learn = rejustify_learn
    if token is None:
        token = rejustify_token
    if email is None:
        email = rejustify_email
    if url is None:
        url = rejustify_main_url

    # convert DataFrame to array with correct naming
    df.loc[-1] = df.columns
    df.index = df.index + 1
    df = df.sort_index()

    # prepare the payload query
    payload = {}
    payload['data'] = df.values.tolist()
    payload['userToken'] = token
    payload['email'] = email
    payload['dataShape'] = shape
    payload['inits'] = inits
    payload['fast'] = fast
    payload['sep'] = sep
    payload['dbAllowed'] = learn

    # send request
    response = requests.post(rejustify_main_url + "/analyze",  data=json.dumps(payload),
                             headers={'Content-Type': 'application/json'})
    # print(payload)
    # testing
    # response = requests.post("http://localhost:5762" + "/analyze", json=payload)
    # print(learn)
    # print(response)

    try:
        response_json = response.json()
    except ValueError:
        raise ValueError("Invalid response from rejustify (JSON expected)")

    if not response.ok:
        raise ValueError(response_json)

    print(response_json)
    # output
    try:
        out = pd.DataFrame(response_json['structure'])
    except:
        out = "Consistency error. Check your input parameters."

    return(out)


def adjust(block=None, column=None, id=None, items=None):
    index = None
    type = "undefined"

    # define block type
    if all(elem in block.columns.values.tolist() for elem in {"id", "column", "name", "empty", "class", "feature", "cleaner", "format", "p_class", "provider", "table", "p_data"}):
        type = "structure"

    # adjust structure
    if type is "structure":
        if items is not None and id is not None:
            index = block['id'].index[id]

        if items is not None and column is not None:
            if(type(column) == "int"):
                index = block['column'].index[column]
            else:
                index = block['name'].index[column]

        block.loc[index, items.keys()] = items.values()

    return(block)
