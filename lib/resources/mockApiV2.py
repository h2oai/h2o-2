import json
import string

fileKeys = []
def postFile(query):
    filename = query.get('filename', ['TEST'])[0]

    response = { "dst" : u".".join(filename.split(".")[:-1])}

    responses = [
            [403,
                json.dumps({
                    "error_msg" : "Key already in use!",
                    "error_code" : 4141
                })
            ],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
        ]

    result = random.choice(responses)

    if result[0] == 200:
        fileKeys.append(response["dst"])

    return result

def getHeader(query):
    uri = query.get('uri', ['TEST'])[0]
    response = {
        "uri": uri,
        "header_separator": random.choice([",", ";", " "])
    }

    responses = [
            [403,
                json.dumps({
                    "error_msg" : "Key already in use!",
                    "error_code" : 4141
                })
            ],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
        ]

    result = random.choice(responses)

    return result

def parsePreview(query):
    def generateSampleColumns():
        result = []

        for i in xrange(random.randint(20000, 20002)):
            column = {}
            column['header'] = "c%d" % i
            column['type'] = random.choice(['ENUM', 'INT', 'DOUBLE'])

            result.append(column)

        return result

    def generateSamplePreview(columns, count):
        result = []

        for i in xrange(count):
            row = []
            for column in columns:
                if column["type"] == "ENUM":
                    row.append(random.choice(string.ascii_uppercase))
                if column["type"] == "INT":
                    row.append(random.randint(-100000, 100000))
                if column["type"] == "DOUBLE":
                    row.append(random.uniform(-100000, 100000))

            result.append(row)

        return result

    response = query

    if not "parser_config" in response:
        response["parser_config"] = {}

    if not "columns" in response["parser_config"]:
        response["parser_config"]["columns"] = generateSampleColumns()

    if not "parser_type" in response["parser_config"]:
        response["parser_config"]["parser_type"] = "CSV"

    if not "header_separator" in response["parser_config"]:
        response["parser_config"]["header_separator"] = ","

    if not "data_separator" in response["parser_config"]:
        response["parser_config"]["data_separator"] = ","

    if not "header_file" in response["parser_config"]:
        response["parser_config"]["header_file"] = random.choice(response["parser_config"]["uris"])

    if not "skip_header" in response["parser_config"]:
        response["parser_config"]["skip_header"] = False

    if response["parser_config"]["skip_header"]:
        response["parser_config"]["header_file"] = None

    if not "dst" in response:
        response["dst"] = u".".join(random.choice(query["parser_config"]["uris"]).split(".")[:-1])

    response["preview_len"] = 10
    response["preview"] = generateSamplePreview(response["parser_config"]["columns"], response["preview_len"])

    responses = [
            [401,
                json.dumps({
                    "error_msg" : "Something went bad.",
                    "error_code" : 4141
                })
            ],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
            [200, json.dumps(response)],
        ]

    result = random.choice(responses)

    return result

JOBS = {}
def parse(query):
    import uuid

    response = {
        'job': unicode(uuid.uuid4())
    }

    responses = [
        [401,
            json.dumps({
                "error_msg" : "Something went bad.",
                "error_code" : 4141
            })
        ],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
    ]

    result = random.choice(responses)

    if result[0] == 200:
        job = response["job"]
        JOBS[job] = {
            'parser_config': query["parser_config"],
            'job_status': {
                "progress" : 0.0,
                "status"   : "inprogress",
                "job"      : job,
                "errors"   : []
            }
        }

    return result

def progress(query):
    job      = query["job"][0]

    print JOBS[job]["parser_config"]["uris"]

    JOBS[job]["job_status"]["progress"] += 0.04

    if JOBS[job]["job_status"]["progress"] >= 1:
        JOBS[job]["job_status"]["progress"] = 1
        JOBS[job]["job_status"]["status"] = "finished"
    elif random.uniform(0.,1.) > 0.95:
        JOBS[job]["job_status"]["errors"].append({
            "file": random.choice(JOBS[job]["parser_config"]["uris"]),
            "msg": "Unknown characters encountered in file."
        })

    response = JOBS[job]["job_status"]

    responses = [
        [401,
            json.dumps({
                "error_msg" : "Something went bad.",
                "error_code" : 4141
            })
        ],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
    ]

    result = random.choice(responses)

    return result

def cancel(query):
    job      = query["job"][0]

    response = "OK"

    responses = [
        [401,
            json.dumps({
                "error_msg" : "Something went bad.",
                "error_code" : 4141
            })
        ],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
        [200, json.dumps(response)],
    ]

    result = random.choice(responses)

    if result[0] == 200:
        del JOBS[job]

    return result


GET_MOCKS = {
    '/v2/list_uri.json':
        [
            (200, """
                {
                    "dst": "dataset",
                    "uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_which_happens_to_be_awfully_long_2.dat.gz",
                            "size": 1024
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_2.dat.gz",
                            "size": 2048
                        }
                    ],
                    "ignored_uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_SUCCESS",
                            "size": 1890010
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_FAILED",
                            "size": 0
                        }
                    ]
                }
            """),
            (200, """
                {
                    "dst": "dataset",
                    "uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_which_happens_to_be_awfully_long_2.dat.gz",
                            "size": 1024
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_2.dat.gz",
                            "size": 2048
                        }
                    ],
                    "ignored_uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_SUCCESS",
                            "size": 1890010
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_FAILED",
                            "size": 0
                        }
                    ]
                }
            """),
            (200, """
                {
                    "dst": "dataset",
                    "uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_which_happens_to_be_awfully_long_2.dat.gz",
                            "size": 1024
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_2.dat.gz",
                            "size": 2048
                        }
                    ],
                    "ignored_uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_SUCCESS",
                            "size": 1890010
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_FAILED",
                            "size": 0
                        }
                    ]
                }
            """),
            (200, """
                {
                    "dst": "dataset",
                    "uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_which_happens_to_be_awfully_long_2.dat.gz",
                            "size": 1024
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/file_2.dat.gz",
                            "size": 2048
                        }
                    ],
                    "ignored_uris": [
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_SUCCESS",
                            "size": 1890010
                        },
                        {
                            "uri": "hdfs://192.168.1.161/home-0xdiag-datasets/_FAILED",
                            "size": 0
                        }
                    ]
                }
            """),
            (401, """
                {
                    "error_msg": "Bad URI!",
                    "error_code": 1111
                }
            """),
        ],
    '/v2/get_header': getHeader,
    '/v2/parse_progress.json': progress,
    '/v2/cancel_job.json': cancel
}

POST_MOCKS = {
    '/v2/parse_preview.json': parsePreview,
    '/v2/parse.json': parse
}

POST_UPLOAD_MOCKS = {
    '/v2/post_file.json': postFile
}

MOCKS = {
    'GET': GET_MOCKS,
    'POST': dict(POST_MOCKS.items() + POST_UPLOAD_MOCKS.items())
}

from inspect import isfunction
import random

def isMockAvailable(path, method):
    return (method in MOCKS and path in MOCKS[method])

def shouldMockUpload(path, method):
    return (method == "POST" and path in POST_UPLOAD_MOCKS)

def defaultMockResponse():
    return [(501, "mock not implemented!")]

def mockResponse(path, params, method):
    mocks = MOCKS.get(method, {})

    mock_responses = mocks.get(path, defaultMockResponse())

    if isfunction(mock_responses):
        return mock_responses(params)
    else:
        return random.choice(mock_responses)
