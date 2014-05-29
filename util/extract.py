# To run: python extract.py vdbe.c out.c

import re

def printout(function, outputFile):
    f = open(outputFile, 'a')
    for line in function:
        f.write(line + '\n')
    f.close()

def deleteBreak(program, pos, endSwitch):
    i = pos + 1
    result = False
    keepGoing = True
    while i < endSwitch and keepGoing:
        line = program[i]
        if re.match('case', line) or re.match('#if', line) or re.match('#endif', line):
            result = True
            break
        elif re.search(r'^\s*\*\*', line) or re.search(r'^\s*\}', line) or re.search(r'^\s*\/\*', line) or re.search(r'^\s*\*\/', line) or re.search(r'^\s*$', line):
            pass
        else:
            keepGoing = False
        i += 1
    if i == endSwitch:
        result = True
    return result

def extract(program):
    function = []
    beginSwitch = 0
    length = len(program)
    while beginSwitch < length and not re.search('switch', str(program[beginSwitch])):
        beginSwitch += 1

    endSwitch = length - 1
    while endSwitch >= 0 and not re.search('default', str(program[endSwitch])):
        endSwitch -= 1

    for i in range(beginSwitch + 1, endSwitch):
        line = program[i]

        if re.match('case', line):
            if re.match('case', program[i + 1]):
                opcode1 = re.match(r"(case)\s(.*):", str(line))
                opcode2 = re.match(r"(case)\s(.*):", str(program[i + 1]))
                function.append('void impl_' + opcode1.group(2) + '(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {')
                function.append('  impl_' + opcode2.group(2) + '(p, db, pc, pOp);')
                function.append('}')
            else:
                opcode = re.match(r"(case)\s(.*):", str(line))
                function.append('void impl_' + opcode.group(2) + '(Vdbe *p, sqlite3 *db, int pc, Op *pOp) {')
        elif re.search(r'\s+[Ff]all\s+through', str(line)) and re.search(r'\s+OP_.*\s+', str(line)):
            opcode = re.search(r"\s+OP_(.*)\s+", str(line))
            function.append('  impl_OP_' + opcode.group(1) + '(p, db, pc, pOp);')
        elif re.search(r'^\s*break;', str(line)):
            if not deleteBreak(program, i, endSwitch):
                function.append(line)
        else:
            function.append(line)
    return function

def run(inputFile, outputFile):
    program = inputFile.read().splitlines()
    printout(extract(program), outputFile)
    
    
if __name__ == "__main__":
    import sys
    import os
    if os.path.exists(sys.argv[2]):
        os.remove(sys.argv[2])
    run(open(sys.argv[1], 'r'), sys.argv[2])

