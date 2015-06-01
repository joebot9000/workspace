CXXFLAGS =	-O2 -g -Wall -fmessage-length=0

OBJS =		test.o

LIBS =

TARGET =	test.exe

$(TARGET):	$(OBJS)
	$(CXX) -o $(TARGET) $(OBJS) $(LIBS)

all:	$(TARGET)

clean:
	rm -f $(OBJS) $(TARGET)
