
import sys
sys.path.extend(['.','..','py'])
import h2o_cloud

def put_file(h2o=None, path=None, key=None, timeoutSecs=60):

    if not h2o:
        h2o = h2o_cloud.nodes[0]

    if not key:
        key = os.path.basename(path)
        ### print "putfile specifying this key:", key

    fileObj = open(path, 'rb')
    resp = h2o.__do_json_request(
        '2/PostFile.json',
        cmd='post',
        timeout=timeoutSecs,
        params={"key": key},
        files={"file": fileObj},
        extraComment=str(path))

    verboseprint("\nput_file response: ", dump_json(resp))
    fileObj.close()
    return key

