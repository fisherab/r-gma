import os, sys

inpat = sys.argv[1]
outpat = sys.argv[2]

print inpat, outpat

for root, dirs, files in os.walk('.'):
    for file in files:
        name,ext = os.path.splitext(file)
        if ext == ".java":
            print name
            fullname = os.path.join(root, file)
            f = open(fullname)
            lines = f.readlines()
            f.close()
            f = open("tmpfile", "w")
            for line in lines:
                line = line.replace(inpat, outpat)
                f.write(line)
            f.close()
            os.remove(fullname)
            os.rename("tmpfile", fullname)
                
                     
    if 'CVS' in dirs:
        dirs.remove('CVS')  # don't visit CVS directories