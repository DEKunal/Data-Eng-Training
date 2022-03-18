a='0,Will,33,385'

def parseline(line):
    fields=line.split(',')
    age=int(fields[2])
    friends=int(fields[3])
    return (age,friends)


print(parseline(a))
