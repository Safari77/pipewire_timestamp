SHELL = /bin/sh
CC = clang
# compile pipewire with -fsanitize-cfi-cross-dso if you want full CFI
CFLAGS = -Og -I/usr/include/pipewire-0.3 -I/usr/include/spa-0.2 -std=gnu23 -ggdb3 -gdwarf-5 -march=native -U_FORTIFY_SOURCE -D_FORTIFY_SOURCE=3 -fPIC -grecord-gcc-switches -fstack-protector-strong -fcf-protection=full -fsanitize=safe-stack,scudo,cfi -fno-sanitize=cfi-icall -flto -fvisibility=hidden --param=ssp-buffer-size=4 -MMD -MP
LDFLAGS = -lavformat -lavcodec -lavfilter -lavutil -lswresample -lm -lz -lpthread -lpipewire-0.3 -pie -Wl,-z,relro -Wl,-z,now -flto -fstack-protector-strong -fsanitize=safe-stack,scudo,cfi -fuse-ld=lld

TARGET  = pipewire_timestamp
SOURCES = pipewire_timestamp.c

OBJECTS = $(SOURCES:.c=.o)
DEPS    = $(OBJECTS:.o=.d)

.PHONY: all clean format

all: $(TARGET)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) $^ $(LDFLAGS) -o $@
	@checksec --file=$@ 2> /dev/null || true

-include $(DEPS)

clean:
	rm -f $(TARGET) $(OBJECTS) $(DEPS) tags

format:
	clang-format -i $(SOURCES) $(wildcard *.h)
