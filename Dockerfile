FROM openjdk:7
WORKDIR /usr/src/CourseProject
COPY . /usr/src/CourseProject
ENV GOOGLE_APPLICATION_CREDENTIALS ./cloudComputingCourseProject-0b00aaec228e.json
RUN javac client.java
CMD ["java", "client"]