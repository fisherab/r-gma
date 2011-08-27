# -*- coding: utf-8 -*-

import sys, os, shutil, getopt

# Define how to run latex

def makeindex(fn):
    run_and_print("makeindex " + fn)

def latex(fn):
    run_and_print("pdflatex " + fn)

def latexQuiet(fn):
    run_and_print("pdflatex " + fn)

def bibtex(fn):
    run_and_print("bibtex " + fn)

def texify(fn):
    """This is the command to run texify.

    It may not be on all tex distributions. If you don't have it just
    avoid the -t option"""

    run_and_print("texify --quiet --pdf " + fn + ".tex")

def tex_by_hand(doc):
    latexQuiet(doc)
    makeindex(doc)
    bibtex(doc)
    latexQuiet(doc)
    latexQuiet(doc)

# Define abbrev - i.e more or less macros    

abbrev = {}
abbrev["doc"] = ["guides", "specification", "design"]
abbrev["guides"] = ["c-guide", "py-guide", "java-guide", "cpp-guide"]

# Define set of actioncodes
  
actioncode = {}

def helpme():
    print """
Usage: build.py [Option...] [Command...]

Commands are actions or macros and the default is "doc".
    
Options:
         -t or --texify to run the tex compiler driver if available
         -d or --debug to just run latex once and show the output
"""
    print "Actions: "
    for action in actioncode:
        print " "*8, action

    print "\nMacros: "
    for key in abbrev.keys():
        print " "*8, key
        
actioncode["help"] = helpme

def clean():
    if os.path.isdir("build"):
        shutil.rmtree("build")
actioncode["clean"] = clean

def specification():
    simple_build("specification")
actioncode["specification"] = specification

def design():
    simple_build("design")    
actioncode["design"] = design

def system_guide():
    simple_build("system-guide")    
actioncode["system-guide"] = system_guide

def c_guide():
    build_guide("c-guide", language_name="C", language = "c", EDMS="503615")
actioncode["c-guide"] = c_guide

def java_guide():
    build_guide("java-guide", language_name="Java", language = "java", EDMS="503617")
actioncode["java-guide"] = java_guide

def py_guide():
    build_guide("py-guide", language_name="Python", language = "py", EDMS="503614")
actioncode["py-guide"] = py_guide

def cpp_guide():
    build_guide("cpp-guide", language_name="C++", language = "cpp", EDMS="503616")
actioncode["cpp-guide"] = cpp_guide

# Define utilities

def copy_if_needed(src,dest):
    for fn in os.listdir(src):
        if os.path.isdir(os.path.join(src,fn)): continue
        if not os.path.exists(os.path.join(dest,fn)):
            shutil.copy(os.path.join(src,fn), dest)
        elif os.stat(os.path.join(dest,fn)).st_mtime < os.stat(os.path.join(src,fn)).st_mtime:
            shutil.copy(os.path.join(src,fn), dest)

def run_and_print(cmd):
    """Run a command with popen3 and print the stderr and stdout if present"""
    child_stdin, child_stdout, child_stderr = os.popen3(cmd)
    print "Running ", cmd
    child_stdin.close()
    stdout = child_stdout.readlines()
    child_stdout.close()
    if len(stdout) > 0:
        print "STDOUT"
    for line in stdout:
        print line.strip()
    stderr = child_stderr.readlines()
    child_stderr.close()
    if len(stderr) > 0:
        print "STDERR"
    for line in stderr:
        print line.strip()

def build_guide(doc,language_name, language, EDMS):
    """Build a guide"""
    builddir = os.path.join("build", "guides")
    if not os.path.isdir(builddir): os.makedirs(builddir)
    copy_if_needed(os.path.join("doc","common"), builddir)
    copy_if_needed(os.path.join("doc", "guides"), builddir)
    pdf = os.path.join(builddir, language + ".pdf")
    build = not os.path.exists(pdf)
    if not build:
        btime = os.stat(os.path.join(pdf)).st_mtime
        for fn in os.listdir(builddir):
            if fn in ["cpp.pdf", "c.pdf", "java.pdf", "py.pdf"]: continue
            root, ext = os.path.splitext(fn)
            if ext in [".aux", ".bbl", ".blg", ".log", ".out", ".toc"]: continue
            end = fn.split("-")[-1]
            if end in ["c.tex", "cpp.tex", "py.tex", "java.tex"]: continue
            if os.stat(os.path.join(builddir,fn)).st_mtime > btime:
                print "Trigger build on", fn
                build = True
                break
    if build or debugR:
        print "Build", doc
        cwd = os.getcwd()
        os.chdir(builddir)
        f = open("latex.template")
        lines = f.readlines()
        f.close()
        f = open(language + ".tex", "w")
        for line in lines:
            line = line.replace("@language-name@", language_name)
            line = line.replace("@language@", language)
            line = line.replace("@EDMS@", EDMS)
            f.write(line)
        f.close()    

        for fn in os.listdir("."):
            root, ext = os.path.splitext(fn)
            if ext != "." + language: continue
            code = file(fn)
            lines = code.readlines()
            code.close()
            tex = file(root + "-" + language + ".tex", "w")
            linenum = 0
            plinenum = 0
            for line in lines:
                tex.write(r"\par\verbÂ£")
                if not line.isspace():
                    tex.write('%3d   %s' % (plinenum ,line[:-1]))
                tex.write(r"Â£")
                if linenum != len(lines) -1:
                    tex.write(r"\vspace{-2mm}")
                tex.write("\n")    
                linenum = linenum + 1
                if not line.isspace():plinenum = plinenum + 1
            tex.write(" \n")
            tex.close()

        if texifyR:
            texify(language)
        elif debugR:
            latex(language)
        else:
            tex_by_hand(language)
        os.chdir(cwd)
        shutil.copy(pdf, os.path.join("doc","built"))
    else:
        print doc, "is already built"

def simple_build(doc):
    """Build a document - but not one of the guides"""
    builddir = os.path.join("build", doc)
    if not os.path.isdir(builddir): os.makedirs(builddir)
    copy_if_needed(os.path.join("doc","common"), builddir)
    copy_if_needed(os.path.join("doc", doc), builddir)
    pdf = os.path.join(builddir, doc + ".pdf")
    build = not os.path.exists(pdf)
    if not build:
        btime = os.stat(os.path.join(pdf)).st_mtime
        for fn in os.listdir(builddir):
            if os.stat(os.path.join(builddir,fn)).st_mtime > btime:
                print "Trigger build on", fn
                build = True
                break
    if build or debugR:
        print "Build", doc
        cwd = os.getcwd()
        os.chdir(builddir)
        if texifyR:
            texify(doc)
        elif debugR:
            latex(doc)
        else:
            tex_by_hand(doc)
        os.chdir(cwd)
        shutil.copy(pdf, os.path.join("doc","built"))
    else:
        print doc, "is already built"

# Main program. Get the options and the actions. Expand the actions
# and run them - with duplicates if the user requests it.

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hdt", ["help", "debugR", "texifyR"])
    except getopt.GetoptError:
        # print help information and exit:
        helpme()
        sys.exit(1)

    global debugR, texifyR    
    debugR = False
    texifyR = False
    for o, a in opts:
        if o in ("-d", "--debug"):
            debugR = True
        if o in ("-t", "--texify"):
            texifyR = True
        if o in ("-h", "--help"):
            helpme()
            sys.exit()

    if debugR and texifyR:
        print "debug and textify are not compatible options"
        sys.exit(1)        

    if len(args) == 0: args = ["doc"]

    actions = expandargs(args)
    for action in actions:
        if action not in actioncode:
            print "Action", action, "not recognised"
            sys.exit(1)

    for action in actions:
        actioncode[action]()
        
    print "Done"

def expandargs(args):
    """Called in main program to expand actions"""
    outargs = []
    for a in args:
        if a in abbrev:
            outargs.extend(expandargs(abbrev[a]))
        else:
            outargs.append(a)
    return outargs
        
if __name__ == "__main__": main()
