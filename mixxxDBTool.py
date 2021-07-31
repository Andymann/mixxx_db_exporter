import sys
import argparse
import os.path
import sqlite3
import shutil
from pathlib import Path

# export:
# python3 ./mixxxDBTool.py -e /home/andy/.mixxx/mixxxdb.sqlite -c ./cache2/
#
# import:
# python3 ./mixxxDBTool.py -i ./cache2/mixxxdb.sqlite -c ./cache2/ -t ./target/mixxxdb.sqlite -r ./target/files

# Testweise Ausgabe der Tabelle 'track_locations'
def readDatabase(pDBFile):
    con = sqlite3.connect(pDBFile)
    cur = con.cursor()

    cur.execute("SELECT id, location, directory FROM track_locations")
    rows = cur.fetchall()
    iCounter = 0
    for name in rows:
        iCounter += 1
        if iCounter>100:
            break
            None
        print(name)
    
    cur.close()
    con.close()


# Sanitycheck on the DBFile. Are there one or more entires in 'track_locatins'?
def checkDatabase(pDBFile):
    con = sqlite3.connect(pDBFile)
    cur = con.cursor()

    cur.execute("SELECT id, location, directory FROM track_locations")
    rows = cur.fetchall()
    iCounter = 0
    for name in rows:
        iCounter += 1
    
    cur.close()
    con.close()
    if iCounter>0:
        print('checkDatabase():' + pDBFile + ' contains ' + str(iCounter) + ' tracks.')
        return True
    else:
        print('checkDatabase():' + pDBFile + ' contains ' + str(iCounter) + ' tracks. that is a problem')
        return False

# For the love of nature, please have only one line in table 'directories':
def getShortestCommonPath(pDBFile):
    print('getShortestCommonPath():' + pDBFile)
    sShortestPath = ''
    bHasCommonPath = True

    con = sqlite3.connect(pDBFile)
    cur = con.cursor()
    iCounter=0

    cur.execute("SELECT directory FROM directories")
    rows = cur.fetchall()
    for row in rows:
        iCounter+=1
        sShortestPath = row[0]
        print(row)
        
    if(iCounter==1):
        print('There is a shortest path for all files in the database:' + sShortestPath)
        sReturn = sShortestPath
    #if bHasCommonPath:
    #    print('There is a shortest path for all files in the database:' + sShortestPath)
    #    sReturn = sShortestPath
    else:
        print('There is no shortest common path for all files in the database:')
    
    cur.close()
    con.close()
    return sShortestPath


# maybe all files in the database have the same root folder(s).
# If so then we can remove them in order to prevent unnecessarily deep 
# directory structures.
def x_getShortestCommonPath(pDBFile):
    print('getShortestCommonPath():' + pDBFile)
    sReturn = None
    iShortestPath = 9999
    sShortestPath = ''
    bHasCommonPath = True

    con = sqlite3.connect(pDBFile)
    cur = con.cursor()

    cur.execute("SELECT id, location, directory FROM track_locations")
    rows = cur.fetchall()
    for row in rows:
        print(row[2])
        if len(row[2])<iShortestPath:
            iShortestPath = len(row[2])
            sShortestPath = row[2]
            print(sShortestPath)
        
    for row in rows:
        if not row[2].startswith(sShortestPath):
            bHasCommonPath = False
            break

    if bHasCommonPath:
        print('There is a shortest path for all files in the database:' + sShortestPath)
        sReturn = sShortestPath
    else:
        print('There is no shortest common path for all files in the database:')
    
    cur.close()
    con.close()
    return sReturn

# Change entries in table 'treack_locations' to match with the newly imported files
def processDatabase(pDBFile, pTargetRootFolder, pCommonShortestPath):
    print('processDatabase()' + pDBFile + ' ' + pTargetRootFolder + ' ' + pCommonShortestPath)
    con = sqlite3.connect(pDBFile)
    cur = con.cursor()

    cur.execute("SELECT id, location, directory FROM track_locations")
    rows = cur.fetchall()
    iCounter=0
    
    for row in rows:
        iCounter+=1
        if iCounter > 200:
            #break
            None

        #print('shortestPath length:' + str(iShortestPath))
        sNewPath = pTargetRootFolder + row[1]

        if not pCommonShortestPath == None:
            sNewPath = pTargetRootFolder + row[1][len(pCommonShortestPath):]

        #print(row[1]+ '\t\t'+ sNewPath)
        #print(os.path.dirname(sNewPath))

        # New row[1]: sNewPath
        # New row[2]: os.path.dirname(sNewPath)

        #print(row[2])
        cur2 = con.cursor()
        sql_update_query = """Update track_locations set location = ?, directory = ? where id = ?"""

        # absolute paths into database
        data = (os.path.abspath(sNewPath), os.path.abspath(os.path.dirname(sNewPath)), row[0])
        cur2.execute(sql_update_query, data)
        con.commit()
        #print("Record Updated successfully")
        cur2.close()

    cur.close()
    con.close()
    

# Read track loactions from Database, copy files
# If there is a common path on every file then we remove it while copying
# in order to prevent directory clutter

def copyTracksToCachedirectory(pDBFile, pDirectory, pCommonShortestPath):
    #if not pCommonShortestPath == None:
    print('copyTracksTocachedirectory():' + pDirectory + ' ' + str(pCommonShortestPath))
        #None
    #else:
    #    print('copyTracksTocachedirectory():')
    con = sqlite3.connect(pDBFile)
    cur = con.cursor()

    cur.execute("SELECT id, location, directory FROM track_locations")
    rows = cur.fetchall()
    iCounter=0
    for row in rows:
        iCounter+=1
        if iCounter > 100:
            #break
            None
        
        sNewPath = row[1]

        if not pCommonShortestPath == None:
            #pCommonShortestPath beginnt mit /
            if not pCommonShortestPath.startswith(os.sep):
                pCommonShortestPath = os.sep + pCommonShortestPath

            # pCommonShortestPath auf jeden Fall ohne / am Ende
            if pCommonShortestPath.endswith(os.sep):
                pCommonShortestPath = pCommonShortestPath[:len(pCommonShortestPath) - 1]

            sNewPath = row[1][len(pCommonShortestPath):]
           
        try:
            #os.makedirs(pDirectory + os.path.dirname(row[1] ), exist_ok=True)
            #shutil.copy2(row[1], pDirectory + os.path.dirname(row[1] ))
            os.makedirs(pDirectory + os.path.dirname(sNewPath), exist_ok=True)
            shutil.copy2(row[1], pDirectory + sNewPath)
            print('Successfully copied ' + row[1] + ' to ' + pDirectory + sNewPath)
        except:
            #print('Error copying ' + row[1] + ' to ' + pDirectory + os.path.dirname(row[1] ))
            print('Error copying ' + row[1] + ' to ' + pDirectory + sNewPath)
        
    cur.close()
    con.close()

# get all files from cachedirectory, copy to target directory, ignore database file (this coudl be WAY better)
def copyTracksToTargetDirectory(pSourceDir, pTargetDir, pDBFile):
    files = []
    iCounter=0
    iCopyCounter=0
    bReturn = True
    # r=root, d=directories, f = files
    for r, d, f in os.walk(pSourceDir):
        for file in f:
            iCounter += 1
            # where are the source files?
            files.append(os.path.join(r, file))

    #print('************   copyTracksToTargetDirectory():Filecopy temporarily disabled   ************')            
    for sourceFile in files:
        # remove pSourceDir from Filepath
        targetFile = sourceFile[len(pSourceDir):]
        #print(pTargetDir + targetFile)
        #print(os.path.dirname(pTargetDir + targetFile))

        if not sourceFile == pDBFile:
            try:
                os.makedirs(os.path.dirname(pTargetDir + targetFile), exist_ok=True)
                shutil.copy2(sourceFile, pTargetDir + targetFile)
                iCopyCounter+=1
                print('copyTracksToTargetDirectory(): successfully copied ' + sourceFile + ' to ' + pTargetDir + targetFile)
            except:
                print('copyTracksToTargetDirectory(): could not copy ' + sourceFile + ' to ' + pTargetDir + targetFile)
                bReturn = False
        else:
            print('copyTracksToTargetDirectory(): Ignoring DBFile')
    print('copyTracksToTargetDirectory(): found ' + str(iCounter) + ' files. Sucessfully copied ' + str(iCopyCounter) + '.')
    return bReturn

# copy file to folder
def copyDatabaseFile(pDBFile, pDirectory):
    try:
        print('copyDatabaseFile():' + pDirectory + os.sep + Path(pDBFile).name)
        os.makedirs(os.path.dirname(pDirectory), exist_ok=True)
        shutil.copy2(pDBFile, pDirectory + os.sep + Path(pDBFile).name)
        return True
    except:
        print('copyDatabaseFile(): could not copy ' + pDBFile)
        return False

#
### Main Export function
#
def exportFiles(pDBFile, pCacheDir):
    print('exportFiles:' + pDBFile + ' ' + pCacheDir)
    if not os.path.isfile(pDBFile):
        print('exportFiles(): no valid db file given')
        return False

    #cacheDir auf jeden Fall ohne / am Ende
    if pCacheDir.endswith(os.sep):
            pCacheDir = pCacheDir[:len(pCacheDir) - 1]

    copyDatabaseFile(pDBFile, pCacheDir + os.sep)

    pCacheDir += os.sep + 'mixxxFileExport'
    #os.makedirs(os.path.dirname(pCacheDir), exist_ok=True)
    os.makedirs(pCacheDir, exist_ok=True)
    if not os.path.isdir(pCacheDir):
        print('exportFiles(): cacheDirectory does not exist and could not be created')
        return False

    if checkDatabase(pDBFile):
        sCommonShortestPath = getShortestCommonPath(pDBFile)
        if not  sCommonShortestPath == None:
            print('exportfiles(): shortestCommonPath:' + sCommonShortestPath)

        print('exportFiles(): Everything went fine so far. Copying tracks now')
        copyTracksToCachedirectory(pDBFile, pCacheDir, sCommonShortestPath)

        return True
    else:
        return False

#
### Main Import function
#
def importFiles(pDBFile, pCacheDir, pTargetDBFile, pTargetRootFolder):
    print('importFiles():' +pDBFile + ' ' + pCacheDir + ' ' + pTargetDBFile+ ' ' + pTargetRootFolder)
    if not os.path.isfile(pDBFile):
        print('importFiles(): no valid db file given')
        return False

    #cacheDir auf jeden Fall ohne / am Ende
    if pCacheDir.endswith(os.sep):
            pCacheDir = pCacheDir[:len(pCacheDir) - 1]

    #targetRootFolder auf jeden Fall ohne / am Ende
    if pTargetRootFolder.endswith(os.sep):
            pTargetRootFolder = pTargetRootFolder[:len(pCacheDir) - 1]
        
    if not os.path.isdir(pCacheDir):
        print('importFiles(): cachedirectory could not be found')
        return False
    
    os.makedirs(pTargetRootFolder, exist_ok=True)
    #if not os.path.exists(pTargetRootFolder):
    if not os.path.isdir(pTargetRootFolder):
        print('importFiles(): Target root folder ' + pTargetRootFolder +' does not exist and could not be created')
        return False

    if checkDatabase(pDBFile):
        #print('importFiles(): Everything went fine so far. Copying tracks now')
        sCommonShortestPath = getShortestCommonPath(pDBFile)
        if not  sCommonShortestPath == None:
            print('importfiles(): shortestCommonPath:' + sCommonShortestPath)

        print('importFiles(): Everything went fine so far. Copying tracks now')
        #copyTracksToCachedirectory(pDBFile, pCacheDir, sCommonShortestPath)
        #copyDatabaseFile(pDBFile, pCacheDir)
        #return True
        
        pCacheDir += os.sep + 'mixxxFileExport'

        # In cachedir ist die flachst moegliche Ordnerstruktur. wir kopieren direkt nach pTargetRootFolder
        if not copyTracksToTargetDirectory(pCacheDir, pTargetRootFolder, pDBFile):
            print('Copy finished but not all tracks could be copied')
        
        try:
            print('copy mixxxdb:' + pDBFile + ' to ' + pTargetDBFile)
            shutil.copy2(pDBFile, pTargetDBFile)
        except:
            print('Error coyping database file')
            return False

        # And for our next presentation....
        processDatabase(pTargetDBFile, pTargetRootFolder, sCommonShortestPath)
        
        return True
    else:
        return False


parser = argparse.ArgumentParser()
parser.add_argument("-e", "--exportdatabase", type=str, help="Export mode. Needs absolute path to mixxx's db-file to export data from (e.g. ~/.mixxx/mixxxdb.sqlite on a linux machine)")
parser.add_argument("-c", "--cachedir", type=str, help="Cache-Directory. Needs absolute path where data are exported to (e.g. USB-stick)")
parser.add_argument("-i", "--importdatabase", type=str, help="Import mode. Needs absolute path to a dbfile that has been exported before")
parser.add_argument("-t", "--targetdatabase", type=str, help="Needs absolute path to where the db file should be copied to on the new machine")
parser.add_argument("-r", "--targetrootfolder", type=str, help="Target root folder wehre tracks should be copied to.")

args = parser.parse_args()
if args.exportdatabase == None and args.importdatabase == None:
    print('You need to provide a mode (import or export)')

elif not args.exportdatabase == None:
    if args.cachedir == None:
        print('Export mode needs -c / --cachedir')
    else:
        print('Export mode')
        if exportFiles(args.exportdatabase, args.cachedir):
            print('Export successful')
        else:
            print('Something went wrong on export')

elif not args.importdatabase == None:
    if args.cachedir == None:
        print('Import mode needs -c / --cachedir')
    else:
        if args.targetdatabase == None:
            print('Import mode needs -t / --targetdatabase')
        else:
            if args.targetrootfolder == None:
                print('Import mode needs -r / --targetrootfolder')
            else:
                print('Import mode')
                if importFiles(args.importdatabase, args.cachedir, args.targetdatabase, args.targetrootfolder):
                    print('Import successful')
                    readDatabase(args.targetdatabase)
                else:
                    print('somethin went wrong on import')