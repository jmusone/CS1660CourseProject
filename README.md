# CS1660CourseProject
This is the final submission for the CS 1660 Course Project. This Readme assumes you can't use my Docker/GCP resources, which I'm pretty sure is correct.

VIDEO UPLOAD: https://youtu.be/TqeiqkMXZFI

Instructions:
1. Download personal JSON file, update Dockerfile accordingly.
2. Create Docker repository and GCP project. 
3. For the GCP project, create a folder in the VM to hold InvertedIndex and topN JAR files. In client.java the folder is called JAR (can be changed on lines 150, 355).
4. In the GCP project VM, upload the data to a folder. In client.java the folder is called Data (can be changed on line 150).
5. Upload two JAR files to the JAR folder. One file is for InvertedIndex.java, which the code calls invertedindex.jar (line 150). The other file is called topN.jar (line 355) and is for topNDriver.java, topNWordsMapper.java, and topNWordsReducer.java.
6.Change variables in client.java to match GCP project.
  -Global variables at lines 38-40, 43
  -lines 150, 355
