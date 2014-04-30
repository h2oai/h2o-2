from pprint import pprint

params = {}

def parseValue(v):
    if v == 'true': return 1
    if v == 'false': return 0
    try:
        float(v)
        return float(v)
    except ValueError:
        if '.' in v:
            return v.split('.')[-1]
        return v


def process(line):
  global params
  if line.strip()[0] == '_': return
  line = line.split('=')
  if len(line) == 1:  # no default value supplied!
    value = None
    name = line[0].split()[-1].strip().strip(';')
  else:
      value = parseValue(line[-1].strip().strip(';'))
      name = line[0].split()[-1].strip()

  if name[0] == '_': return

  params[name] = value


def main():
    global params
    with open("../src/main/java/hex/singlenoderf/SpeeDRF.java", 'r') as f:
        readnext = False
        for line in f:
            if readnext:
                process(line)
                readnext = False
                continue
            if "@API" in line:
                readnext = True
    pprint(params)


if __name__ == "__main__":
    main()
