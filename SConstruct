
# build file for 10gen db
# this request scons
# you can get from http://www.scons.org
# then just type scons

# some common tasks
#   build 64-bit mac and pushing to s3
#      scons --64 --dist=osx-x86_64 s3dist
#      all s3 pushes require settings.py

import os
import sys
import types 
import re

# --- options ----
AddOption('--prefix',
          dest='prefix',
          type='string',
          nargs=1,
          action='store',
          metavar='DIR',
          help='installation prefix')

AddOption( "--64",
           dest="force64",
           type="string",
           nargs=0,
           action="store",
           help="whether to force 64 bit" )


AddOption( "--32",
           dest="force32",
           type="string",
           nargs=0,
           action="store",
           help="whether to force 32 bit" )


AddOption( "--mm",
           dest="mm",
           type="string",
           nargs=0,
           action="store",
           help="use main memory instead of memory mapped files" )


AddOption( "--release",
           dest="release",
           type="string",
           nargs=0,
           action="store",
           help="relase build")


AddOption( "--dist",
           dest="dist",
           type="string",
           nargs=1,
           action="store",
           help="dist name")


AddOption('--java',
          dest='javaHome',
          type='string',
          default="/opt/java/",
          nargs=1,
          action='store',
          metavar='DIR',
          help='java home')

AddOption( "--v8" ,
           dest="v8home",
           type="string",
           default="../v8/",
           nargs=1,
           action="store",
           metavar="dir",
           help="v8 location")

AddOption( "--noOptimization",
           dest="noOptimization",
           type="string",
           nargs=0,
           action="store",
           help="don't compile with optimization" )

# --- environment setup ---

env = Environment()

env.Append( CPPPATH=[ "." ] )


boostLibs = [ "thread" , "filesystem" , "program_options" ]

commonFiles = Split( "stdafx.cpp db/jsobj.cpp db/json.cpp db/commands.cpp db/lasterror.cpp db/nonce.cpp" )
commonFiles += [ "util/background.cpp" , "util/mmap.cpp" ,  "util/sock.cpp" ,  "util/util.cpp" ]
commonFiles += Glob( "util/*.c" ) + Glob( "grid/*.cpp" )
commonFiles += Split( "client/connpool.cpp client/dbclient.cpp client/model.cpp" ) 

#mmap stuff

if GetOption( "mm" ) != None:
    commonFiles += [ "util/mmap_mm.cpp" ]
elif os.sys.platform == "win32":
    commonFiles += [ "util/mmap_win.cpp" ]
else:
    commonFiles += [ "util/mmap_posix.cpp" ]

coreDbFiles = Split( "" )

serverOnlyFiles = Split( "db/query.cpp db/introspect.cpp db/btree.cpp db/clientcursor.cpp db/javajs.cpp db/tests.cpp db/repl.cpp db/btreecursor.cpp db/cloner.cpp db/namespace.cpp db/matcher.cpp db/dbcommands.cpp db/dbeval.cpp db/dbwebserver.cpp db/dbinfo.cpp db/dbhelpers.cpp db/instance.cpp db/pdfile.cpp db/cursor.cpp db/security_commands.cpp db/security.cpp util/miniwebserver.cpp" )

allClientFiles = commonFiles + coreDbFiles + [ "client/clientOnly.cpp" ];

nix = False
useJavaHome = False
linux64  = False
darwin = False
force64 = not GetOption( "force64" ) is None
force32 = not GetOption( "force32" ) is None
release = not GetOption( "release" ) is None
noOptimization = not GetOption( "noOptimization" ) is None

DEFAULT_INSTALl_DIR = "/usr/local"
installDir = DEFAULT_INSTALl_DIR
nixLibPrefix = "lib"

javaHome = GetOption( "javaHome" )
javaVersion = "i386";
javaLibs = []

distBuild = GetOption( "dist" ) is not None
if distBuild:
    release = True
    installDir = "mongo-db-" + GetOption( "dist" ) + "-latest"

if GetOption( "prefix" ):
    installDir = GetOption( "prefix" )

def findVersion( root , choices ):
    for c in choices:
        if ( os.path.exists( root + c ) ):
            return root + c
    raise "can't find a version of [" + root + "] choices: " + choices

if "darwin" == os.sys.platform:
    darwin = True

    env.Append( CPPPATH=[ "-I/System/Library/Frameworks/JavaVM.framework/Versions/CurrentJDK/Headers/" ] )

    env.Append( CPPFLAGS=" -mmacosx-version-min=10.4 " )
    env.Append( FRAMEWORKS=["JavaVM"] )

    if os.path.exists( "/usr/bin/g++-4.2" ):
        env["CXX"] = "g++-4.2"

    nix = True
    
    if force64:
        env.Append( CPPPATH=["/usr/64/include"] )
        env.Append( LIBPATH=["/usr/64/lib"] )
        if installDir == DEFAULT_INSTALl_DIR:
            installDir = "/usr/64/"
    else:
        env.Append( CPPPATH=[ "/sw/include" , "/opt/local/include"] )
        env.Append( LIBPATH=["/sw/lib/", "/opt/local/lib"] )

elif "linux2" == os.sys.platform:
    useJavaHome = True
    javaOS = "linux"

    if not os.path.exists( javaHome ):
        #fedora standarm jvm location
        javaHome = "/usr/lib/jvm/java/"

    if os.uname()[4] == "x86_64" and not force32:
        linux64 = True
        javaVersion = "amd64"
        nixLibPrefix = "lib64"
        env.Append( LIBPATH=["/usr/lib64"] )
    
    if force32:
        env.Append( LIBPATH=["/usr/lib32"] )

    nix = True

elif "sunos5" == os.sys.platform:
     nix = True
     useJavaHome = True
     javaHome = "/usr/lib/jvm/java-6-sun/"
     javaOS = "solaris"
     env.Append( CPPDEFINES=[ "__linux__" ] )

elif "win32" == os.sys.platform:
    boostDir = "C:/Program Files/Boost/boost_1_35_0"
    javaHome = findVersion( "C:/Program Files/java/" , 
                            [ "jdk" , "jdk1.6.0_10" ] )
    winSDKHome = findVersion( "C:/Program Files/Microsoft SDKs/Windows/" , 
                              [ "v6.0" , "v6.0a" , "v6.1" ] )

    env.Append( CPPPATH=[ boostDir , javaHome + "/include" , javaHome + "/include/win32" , "pcre-7.4" , winSDKHome + "/Include" ] )

    # /Fo"Debug\\" /Fd"Debug\vc90.pdb" 

    env.Append( CPPFLAGS=" /Od /EHsc /Gm /RTC1 /MDd /ZI  /W3 " )
    env.Append( CPPDEFINES=["WIN32","_DEBUG","_CONSOLE","_CRT_SECURE_NO_WARNINGS","HAVE_CONFIG_H","PCRE_STATIC","_UNICODE","UNICODE" ] )

    env.Append( LIBPATH=[ boostDir + "/Lib" , javaHome + "/Lib" , winSDKHome + "/Lib" ] )
    javaLibs += [ "jvm" ];

    def pcreFilter(x):
        name = x.name
        if x.name.endswith( "dftables.c" ):
            return False
        if x.name.endswith( "pcredemo.c" ):
            return False
        if x.name.endswith( "pcretest.c" ):
            return False
        if x.name.endswith( "unittest.cc" ):
            return False
        if x.name.endswith( "pcregrep.c" ):
            return False
        return True

    commonFiles += filter( pcreFilter , Glob( "pcre-7.4/*.c"  ) )
    commonFiles += filter( pcreFilter , Glob( "pcre-7.4/*.cc" ) )
    
    env.Append( LIBS=Split("ws2_32.lib kernel32.lib user32.lib gdi32.lib winspool.lib comdlg32.lib advapi32.lib shell32.lib ole32.lib oleaut32.lib uuid.lib odbc32.lib odbccp32.lib" ) )

else:
    print( "No special config for [" + os.sys.platform + "] which probably means it won't work" )

if useJavaHome:
    env.Append( CPPPATH=[ javaHome + "include" , javaHome + "include/" + javaOS ] )
    env.Append( LIBPATH=[ javaHome + "jre/lib/" + javaVersion + "/server" , javaHome + "jre/lib/" + javaVersion ] )

    javaLibs += [ "java" , "jvm" ]

    env.Append( LINKFLAGS="-Xlinker -rpath -Xlinker " + javaHome + "jre/lib/" + javaVersion + "/server" )
    env.Append( LINKFLAGS="-Xlinker -rpath -Xlinker " + javaHome + "jre/lib/" + javaVersion  )


if nix:
    env.Append( CPPFLAGS="-fPIC -fno-strict-aliasing -ggdb -pthread -Wall -Wsign-compare -Wno-non-virtual-dtor" )
    env.Append( LINKFLAGS=" -fPIC " )
    env.Append( LIBS=[ "stdc++" ] )

    if noOptimization:
        env.Append( CPPFLAGS=" -O0" )
    else:
        env.Append( CPPFLAGS=" -O3" )

    if force64:
        env.Append( CFLAGS="-m64" )
        env.Append( CXXFLAGS="-m64" )
        env.Append( LINKFLAGS="-m64" )

    if force32:
        env.Append( CFLAGS="-m32" )
        env.Append( CXXFLAGS="-m32" )
        env.Append( LINKFLAGS="-m32" )


# --- check system ---

def doConfigure( myenv , java=True , pcre=True , shell=False ):
    conf = Configure(myenv)
    myenv["LINKFLAGS_CLEAN"] = myenv["LINKFLAGS"]
    myenv["LIBS_CLEAN"] = myenv["LIBS"]
    
    def myCheckLib( poss , failIfNotFound=False ):

        if type( poss ) != types.ListType :
            poss = [poss]

        if darwin and release:
            allPlaces = [];
            allPlaces += myenv["LIBPATH"]
            if not force64:
                allPlaces += [ "/usr/lib" , "/usr/local/lib" ]

            for p in poss:
                for loc in allPlaces:
                    fullPath = loc + "/lib" + p + ".a"
                    if os.path.exists( fullPath ):
                        myenv.Append( LINKFLAGS=" " + fullPath + " " )
                        return True

        res = conf.CheckLib( poss )
        if not res and failIfNotFound:
            print( "can't find " + str( poss ) )
            Exit(1)

        return res

    if pcre and not conf.CheckCXXHeader( 'pcrecpp.h' ):
        print( "can't find pcre" )
        Exit(1)

    if not conf.CheckCXXHeader( "boost/filesystem/operations.hpp" ):
        print( "can't find boost headers" )
        Exit(1)

    for b in boostLibs:
        l = "boost_" + b
        myCheckLib( [ l + "-mt" , l ] , not shell)

    if java:
        for j in javaLibs:
            myCheckLib( j , True )

    if nix and pcre:
        myCheckLib( "pcrecpp" , True )
        myCheckLib( "pcre" , True )


    # this will add it iff it exists and works
    myCheckLib( "boost_system-mt" )

    return conf.Finish()

env = doConfigure( env )
# --- v8 ---

v8Home = GetOption( "v8home" )

if not os.path.exists( v8Home ):
    for poss in [ "../v8" , "../open-source/v8" ]:
        if os.path.exists( poss ):
            v8Home = poss
            break

# --- js concat ---

def concatjs(target, source, env):
    
    outFile = str( target[0] )
    
    fullSource = ""

    for s in source:
        f = open( str(s) , 'r' )
        for l in f:
            fullSource += l
            
    out = open( outFile , 'w' )
    out.write( fullSource )

    return None

jsBuilder = Builder(action = concatjs,
                    suffix = '.jsall',
                    src_suffix = '.js')

env.Append( BUILDERS={'JSConcat' : jsBuilder})

# --- jsh ---

def jsToH(target, source, env):
    
    outFile = str( target[0] )
    if len( source ) != 1:
        raise Exception( "wrong" )
    
    h = "const char * jsconcatcode = \n"
    
    for l in open( str(source[0]) , 'r' ):
        l = l.strip()
        l = l.partition( "//" )[0]
        l = l.replace( '\\' , "\\\\" )
        l = l.replace( '"' , "\\\"" )
        

        h += '"' + l + "\\n\"\n "
        
    h += ";\n\n"

    out = open( outFile , 'w' )
    out.write( h )

    return None

jshBuilder = Builder(action = jsToH,
                    suffix = '.jsh',
                    src_suffix = '.jsall')

env.Append( BUILDERS={'JSHeader' : jshBuilder})


# --- targets ----

clientEnv = env.Clone();
clientEnv.Append( CPPPATH=["../"] )
clientEnv.Append( LIBS=[ "libmongoclient.a"] )
clientEnv.Append( LIBPATH=["."] )

testEnv = env.Clone()
testEnv.Append( CPPPATH=["../"] )
testEnv.Append( LIBS=[ "unittest" , "libmongotestfiles.a" ] )
testEnv.Append( LIBPATH=["."] )


# ----- TARGETS ------


# main db target
Default( env.Program( "mongod" , commonFiles + coreDbFiles + serverOnlyFiles + [ "db/db.cpp" ]  ) )

# tools
allToolFiles = allClientFiles + [ "tools/Tool.cpp" ]
env.Program( "mongodump" , allToolFiles + [ "tools/dump.cpp" ] )
env.Program( "mongoimport" , allToolFiles + [ "tools/import.cpp" ] )
env.Program( "mongoimportjson" , allToolFiles + [ "tools/importJSON.cpp" ] )

# dbgrid
env.Program( "mongogrid" , commonFiles + coreDbFiles + Glob( "dbgrid/*.cpp" ) )

# c++ library
env.Library( "mongoclient" , allClientFiles )
env.Library( "mongotestfiles" , commonFiles + coreDbFiles + serverOnlyFiles )

clientTests = []

# examples
clientTests += [ clientEnv.Program( "firstExample" , [ "client/examples/first.cpp" ] ) ]
clientTests += [ clientEnv.Program( "secondExample" , [ "client/examples/second.cpp" ] ) ]
clientTests += [ clientEnv.Program( "whereExample" , [ "client/examples/whereExample.cpp" ] ) ]
clientTests += [ clientEnv.Program( "authTest" , [ "client/examples/authTest.cpp" ] ) ]

# testing
test = testEnv.Program( "test" , Glob( "dbtests/*.cpp" ) )
clientTests += [ clientEnv.Program( "clientTest" , [ "client/examples/clientTest.cpp" ] ) ]


# --- shell ---
# shell is complicated by the fact that v8 doesn't work 64-bit yet

shellEnv = env.Clone();

shellEnv.Append( CPPPATH=[ "../" , v8Home + "/include/" ] )
shellEnv.Append( LIBPATH=[ v8Home] )

if darwin and release and force64:
    shellEnv["LINKFLAGS"] = env["LINKFLAGS_CLEAN"]
    shellEnv["LIBS"] = env["LIBS_CLEAN"]

shellEnv.Append( LIBS=[ "v8" , "readline" ] )

shellEnv.JSConcat( "shell/mongo.jsall"  , Glob( "shell/*.js" ) )
shellEnv.JSHeader( "shell/mongo.jsall" )

def removeIfInList( lst , thing ):
    if thing in lst:
        lst.remove( thing )

if linux64 or force64:
    if linux64:
        shellEnv.Append( CFLAGS="-m32" )
        shellEnv.Append( CXXFLAGS="-m32" )
        shellEnv.Append( LINKFLAGS="-m32" )
        shellEnv.Append( LIBPATH=[ "/usr/lib32" , "/usr/lib" ] )
    else:
        shellEnv["CFLAGS"].remove("-m64")
        shellEnv["CXXFLAGS"].remove("-m64")
        shellEnv["LINKFLAGS"].remove("-m64")
        shellEnv["CPPPATH"].remove( "/usr/64/include" )
        shellEnv["LIBPATH"].remove( "/usr/64/lib" )
        shellEnv.Append( CPPPATH=[ "/sw/include" , "/opt/local/include"] )
        shellEnv.Append( LIBPATH=[ "/sw/lib/", "/opt/local/lib"] )
        
    l = shellEnv["LIBS"]
    if linux64:
        l.remove("java")
        l.remove("jvm")

    removeIfInList( l , "pcre" )
    removeIfInList( l , "pcrecpp" )

    shell32BitFiles = Glob( "shell/*.cpp" )
    for f in allClientFiles:
        shell32BitFiles.append( "32bit/" + str( f ) )

    shellEnv.VariantDir( "32bit" , "." )

    shellEnv = doConfigure( shellEnv , pcre=False , java=False , shell=True )

    shellEnv.Program( "mongo" , shell32BitFiles )
else:
    shellEnv.Append( LIBPATH=[ "." ] )
    shellEnv.Append( LIBS=[ "mongoclient"] )
    shellEnv.Program( "mongo" , Glob( "shell/*.cpp" ) );

#  ---- RUNNING TESTS ----

def testSetup( env , target , source ):
    Mkdir( "/tmp/unittest/" )

testEnv.Alias( "smoke", [ "test" ] , [ testSetup , test[ 0 ].abspath ] )
testEnv.AlwaysBuild( "smoke" )

clientExec = [ x[0].abspath for x in clientTests ];
testEnv.Alias( "smokeClient" , clientExec , clientExec )
testEnv.AlwaysBuild( "smokeClient" )

#  ----  INSTALL -------


#binaries
env.Install( installDir + "/bin" , "mongodump" )
env.Install( installDir + "/bin" , "mongoimportjson" )
env.Install( installDir + "/bin" , "mongod" )
env.Install( installDir + "/bin" , "mongo" )

# NOTE: In some cases scons gets confused between installation targets and build
# dependencies.  Here, we use InstallAs instead of Install to prevent such confusion
# on a case-by-case basis.

#headers
for id in [ "", "util/", "grid/", "db/" ]:
    env.Install( installDir + "/include/mongo/" + id , Glob( id + "*.h" ) )
env.Install( installDir + "/include/mongo/client" , "client/connpool.h" )
env.Install( installDir + "/include/mongo/client" , "client/model.h" )
env.InstallAs( target=installDir + "/include/mongo/client" , source="client/dbclient.h" )

#lib
env.Install( installDir + "/" + nixLibPrefix, "libmongoclient.a" )
env.Install( installDir + "/" + nixLibPrefix + "/mongo/jars" , Glob( "jars/*" ) )

#final alias
env.Alias( "install" , installDir )

#  ---- CONVENIENCE ----

def tabs( env, target, source ):
    from subprocess import Popen, PIPE
    from re import search, match
    diff = Popen( [ "git", "diff", "-U0", "origin", "master" ], stdout=PIPE ).communicate()[ 0 ]
    sourceFile = False
    for line in diff.split( "\n" ):
        if match( "diff --git", line ):
            sourceFile = not not search( "\.(h|hpp|c|cpp)\s*$", line )
        if sourceFile and match( "\+ *\t", line ):
            return True
    return False
env.Alias( "checkSource", [], [ tabs ] )
env.AlwaysBuild( "checkSource" )

def gitPush( env, target, source ):
    import subprocess
    return subprocess.call( [ "git", "push" ] )
env.Alias( "push", [ ".", "smoke", "checkSource" ], gitPush )
env.AlwaysBuild( "push" )


# ---- deploying ---

def s3push( localName , remoteName=None , remotePrefix="-latest" , fixName=True ):
    sys.path.append( "." )

    import simples3
    import settings

    s = simples3.S3Bucket( "mongodb" , settings.id , settings.key )
    un = os.uname()

    if remoteName is None:
        remoteName = localName
        
    if fixName:
        name = remoteName + "-" + un[0] + "-" + un[4] + remotePrefix
        name = name.lower()
    else:
        name = remoteName

    s.put( name  , open( localName ).read() , acl="public-read" );
    print( "uploaded " + localName + " to http://s3.amazonaws.com/" + s.name + "/" + name )

def s3shellpush( env , target , source ):
    s3push( "mongo" , "mongo-shell" )

env.Alias( "s3shell" , [ "mongo" ] , [ s3shellpush ] )
env.AlwaysBuild( "s3shell" )

def s3dist( env , target , source ):
    if not distBuild:
        print( "can't do s3dist without --dist" )
        Exit(1)
        
    s3push( distFile , fixName=False )

distFile = installDir + ".tgz" 
env.Append( TARFLAGS=" -z " )
env.Tar( distFile , installDir )

env.Alias( "s3dist" , [ "install"  , distFile ] , [ s3dist ] )
env.AlwaysBuild( "s3dist" )



