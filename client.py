from threading import Thread
import argparse
import socket
import errno
import time
import os

BUFFER_SIZE = 2048
bytesPerConnection = 0		# Holds the Approximate number of bytes per connection
contentLength = 0			# Holds the content length that is recieved by the HEAD request
downloadedDataThreads = []	# Holds the intermediate starting point (how much data is downloaded)
hiddenFiles = []			# Holds the names of the files which contain the data
path = ""
endBytes = []				# Holds the end byte that the thread has to download

# File Extensions of the Intermediate files created
LOG_FILE_EXTENSION = ".log"
END_BYTE_FILE_EXTENSION = ".eby"
INT_BYTE_FILE_EXTENSION = ".iby"

# Creates Threads and makes .log and .eby files
def threadedDownloading(serverIP, serverPort, connections, location, filename, resumeable):
	global contentLength, bytesPerConnection, downloadedDataThreads, endBytes
	threads = []
	startLength = []
	startByte = 0

	print("The Number of Connections Required are: ", connections)
	print("Approximate Bytes Per Connections: ", bytesPerConnection)
	for i in range(connections):
		print("Connection # ", i + 1)
		startLength.insert(i, startByte)
		startByte += bytesPerConnection
		start = startLength[i]
		end = startLength[i] + bytesPerConnection - 1

		# For Last Thread
		if i == connections - 1:
			end = contentLength - 1

		endBytes[i] = end
		threads.append(Thread(target= createHiddenTemporaryFiles, args=(serverIP, serverPort, location, filename, start, end, i, downloadedDataThreads[i], resumeable)))
		threads[i].start()

		# contains end bytes
		tempEndBytesFile = open(location + "/" + filename + END_BYTE_FILE_EXTENSION, "w+")
		for endbyte in endBytes:
			tempEndBytesFile.write(str(endbyte)+"\n")
		tempEndBytesFile.close()

		# Download Temp File - holds the names of the files
		tempFile = open(location + "/" + filename + LOG_FILE_EXTENSION, "w+")
		for file in hiddenFiles:
			tempFile.write(file+"\n")
		tempFile.close()

	# Waits for all threads to finish
	for thread in threads:
		thread.join()

	# Merge Hidden Files And then delete them
	mergeHiddenFiles(location, filename)

# All threads access this function
# Creates the hidden files (Name Starting with ".filename.ext") and creates a GET Query for each thread
def createHiddenTemporaryFiles(serverIP, serverPort, location, file, start, end, threadID, intermediate, resumeable):
	global hiddenFiles, path, downloadedDataThreads
	filename, extension = file.split(".")
	filename = "." + filename + "(" + str(start) + "-" + str(end) + ")." + extension
	print(filename)				# Hidden Temporary File Printing (Which thread handles this file)

	if resumeable:
		print("Resuming...")
		makeFile = open(location + "/" + filename, "ab+")
	else:
		print("Downloading...")
		makeFile = open(location + "/" + filename, "wb+")

	hiddenFiles.insert(threadID, location + "/" + filename)
	request = bytes("GET /" + path + " HTTP/1.1\r\nHost: " + serverIP + "\r\nRange: bytes=" + str(intermediate) + "-" + str(end) + "\r\n\r\n", "utf-8")
	print(request)

	downloadedDataThreads[threadID] = intermediate
	endBytes[threadID] = end
	makeFile = TCPDownload(serverIP, serverPort, request, makeFile, location, filename, start, end, threadID)

	# contains intermediate bytes
	tempIntBytesFile = open(location + "/" + file + INT_BYTE_FILE_EXTENSION, "w+")
	for intByte in downloadedDataThreads:
		tempIntBytesFile.write(str(intByte)+"\n")
	tempIntBytesFile.close()
	makeFile.close()

# Util Function
def isMultiConnectionPossible(header):
	if b"content-length" in header:
		print("Multi-Connection is Possible")
	else:
		print("Multi-Connection is not Possible")

# Queries TCP & Makes the .iby Temp files
def TCPDownload(serverIP, serverPort, request, file, location, filename, start, end, threadID):
	global downloadedDataThreads
	recievedLength = 0
	headerRecievedFlag = False
	clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientSocket.connect((serverIP,serverPort))
	clientSocket.send(request)

	while True:
		response = clientSocket.recv(BUFFER_SIZE)
		if not response:
			break;
		if not headerRecievedFlag:
			response = response.split(b"\r\n\r\n")[1]
			headerRecievedFlag = True

		file.write(response)
		recievedLength += len(response)
		downloadedDataThreads[threadID] = recievedLength + start - 1
	clientSocket.close()

	return file

# Queries UDP & Makes the .iby Temp files
def UDPDownload(serverIP, serverPort, request, file, location, filename, start, end, threadID):
	global downloadedDataThreads
	recievedLength = 0
	headerRecievedFlag = False
	clientSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	clientSocket.sendto(request, (serverIP, serverPort))

	while True:
		response = clientSocket.recvfrom(BUFFER_SIZE)
		if not response:
			break;
		if not headerRecievedFlag:
			response = response.split(b"\r\n\r\n")[1]
			headerRecievedFlag = True

		file.write(response)
		recievedLength += len(response)
		downloadedDataThreads[threadID] = recievedLength + start - 1
	clientSocket.close()

	return file

# Reads the Header of the Request and saves the content Length in a global variable
def getContentLength(headerList):
	global contentLength
	for header in headerList:
		if b"content-length" in header:
			contentLength = int(header.split(b":")[1])

	print("Content Length: ", contentLength)

# returns the header content in the form of bytes list
def getHeader(serverIP, serverPort, request):
	clientSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
	clientSocket.connect((serverIP,serverPort))
	clientSocket.sendall(request)					# makes code robust to errors (sends again if error)
	header = clientSocket.recv(BUFFER_SIZE)
	header = header.lower()
	clientSocket.close()
	return header

# Merges the data from the Temporary Hidden Files
def mergeHiddenFiles(location, filename):
	global hiddenFiles

	with open(location + "/" + filename, 'wb') as mainFile:
		for file in hiddenFiles:
			reader = open(file, 'rb')
			mainFile.write(reader.read())

	deleteHiddenFiles(location, filename)

# Deletes the Temporary Hidden Files
def deleteHiddenFiles(location, filename):
	global hiddenFiles
	os.remove(location + "/" + filename + LOG_FILE_EXTENSION)	# remove the .log file (contains the hiddenFiles list)
	os.remove(location + "/" + filename + END_BYTE_FILE_EXTENSION)
	os.remove(location + "/" + filename + INT_BYTE_FILE_EXTENSION)
	for file in hiddenFiles:						# remove files from the hiddenFiles list
		if os.path.exists(file):
			os.remove(file)

# Prints the stats of the downloading
def printing(connections, interval, contentLength, startTime):
	global downloadedDataThreads, endBytes
	isFinished = [False] * connections
	allThreads = 0

	while True:
		time.sleep(interval)
		for i in range(connections):
			if isFinished[i]:
				continue
			if downloadedDataThreads[i] >= endBytes[i]:
				downloadedDataThreads[i] = endBytes[i]
				isFinished[i] = True
			print("Connection # " + str(i+1) + "\t:\t" + str(downloadedDataThreads[i]) + "\t/  " + str(endBytes[i]) + "\t\tDownload Speed: " + str(downloadedDataThreads[i]/((time.time() - startTime) * 1000) ) + "\tKB/s")
			allThreads += downloadedDataThreads[i]
		if allThreads > contentLength:
			allThreads = contentLength
		print("Total\t\t:\t" + str(allThreads) + "\t/  " + str(contentLength) + "\t\tDownload Speed: " +  str(allThreads/((time.time() - startTime) * 1000)) + "\tKB/s\n")
		if sum(isFinished) == len(isFinished) or (allThreads >= contentLength):
			break

def main():
	global path, contentLength, bytesPerConnection
	global endBytes, downloadedDataThreads
	# Arguments are handled
	connections, interval, type, url, location, resume = argumentHandling()
	startTime = time.time()

	serverIP, serverPort = url.split("/")[2].split(":")
	serverPort = int(serverPort)
	path = "/".join(url.split("/")[3:])
	filename = path.split("/")[-1]
	# Handles the directories which are not present (deals with the -o flag location)
	print("Creating Directory: " + location)
	directoryHandling(location)
	# only gets the Header
	request = bytes("HEAD /" + path + " HTTP/1.1\r\nHOST: " + serverIP + "\r\n\r\n", "utf-8")
	print(b"Request: " + request)

	header = getHeader(serverIP, serverPort, request)
	if connections >= 2:
		isMultiConnectionPossible(header)
	headerSplit = header.split(b"\n")
	getContentLength(headerSplit)

	# Estimated Bytes Per Connection
	bytesPerConnection = contentLength // connections
	# Initializes all Lists with 0 (lenght equals the number of connections)
	downloadedDataThreads = [0] * connections
	endBytes = [0] * connections
	resumeable = False

	if resume and b'accept-ranges' in header:
		resumeable, downloadedDataThreads, endBytes = isResumable(location, filename)

	print("Resumable: "+str(resumeable))

	# Prints the Stats
	printingThread = Thread(target= printing, args=(connections, interval, contentLength, startTime))
	printingThread.start()

	# time.sleep(1)
	threadedDownloading(serverIP, serverPort, connections, location, filename, resumeable)

# Tells if the downloading is resumable
def isResumable(location, filename):
	global downloadedDataThreads, endBytes, hiddenFiles
	file = location + "/" + filename

	resumeable = False

	file = file + INT_BYTE_FILE_EXTENSION
	if os.path.exists(file):
		downloadedDataThreads = []

	if os.path.exists(file) and file[-4:] == INT_BYTE_FILE_EXTENSION:
		with open(file, 'r') as intermediateFile:
			for line in intermediateFile.readlines():
				intFile = int (line.strip())
				if not os.path.exists(intermediateFile.name):
					downloadedDataThreads = []
					break
				downloadedDataThreads.append(intFile)
			else:
				resumeable = True
	file = file[:-4] + END_BYTE_FILE_EXTENSION
	if os.path.exists(file):
		endBytes = []

	if os.path.exists(file) and file[-4:] == END_BYTE_FILE_EXTENSION:
		with open(file, 'r') as endByteFile:
			for line in endByteFile.readlines():
				endFile = int (line.strip())
				if not os.path.exists(endByteFile.name):
					endBytes = []
					break
				endBytes.append(endFile)
			else:
				resumeable = True
	file = file[:-4] + LOG_FILE_EXTENSION

	if os.path.exists(file) and (file[-4:] == LOG_FILE_EXTENSION):
		with open(file, 'r') as readLogFile:
			for line in readLogFile.readlines():
				temp_file = line.strip()
				hiddenFiles.append(temp_file)
			else:
				resumeable = True

	return resumeable, downloadedDataThreads, endBytes

# Creates Directory If it Does not Exists
def directoryHandling(location):
	if not os.path.exists(location):
		try:
			os.makedirs(location)
		except OSError as err:
			if err.errno != errno.EEXIST:
				raise

# Handles the Command Line Arguments
def argumentHandling():
	parser = argparse.ArgumentParser()
	parser.add_argument("-n", "--connections", help="Total number of simultaneous connections", type = int, required = True)
	parser.add_argument("-i", "--interval", help="Time interval in seconds between metric reporting",type = float, required = True)
	parser.add_argument("-c", "--type", help="Type of connection: UDP or TCP", type = str, required = True)
	parser.add_argument("-f", "--url", help="Address pointing to the file location on the web", type = str, required = True)
	parser.add_argument("-o", "--location", help="Address pointing to the location where the file is downloaded", type = str, required = True)
	parser.add_argument("-r", "--resume", help="Whether to resume the existing download in progress", action="store_true")

	args = parser.parse_args()

	print("Number of Connections\t:\t", str(args.connections))
	print("Time Interval\t\t:\t", str(args.interval))
	print("Transport Protocol\t:\t", str(args.type))
	print("Remote URL\t\t:\t", str(args.url))
	print("Location\t\t:\t", str(args.location))
	print("Resume Capability\t:\t", args.resume)

	return args.connections, args.interval, args.type, args.url, args.location, args.resume

if __name__ == '__main__':
	main()