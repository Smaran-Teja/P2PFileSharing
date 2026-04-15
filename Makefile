SRC_DIR = src
OUT_DIR = out
SOURCES = $(wildcard $(SRC_DIR)/*.java)

all: $(OUT_DIR)
	javac -d $(OUT_DIR) $(SOURCES)

$(OUT_DIR):
	mkdir -p $(OUT_DIR)

run:
	@echo "Usage: make run-node IP=<ip> PORT=<port> [BOOTSTRAP_IP=<ip> BOOTSTRAP_PORT=<port>]"
	@if [ -n "$(BOOTSTRAP_IP)" ] && [ -n "$(BOOTSTRAP_PORT)" ]; then \
		java -cp $(OUT_DIR) Main $(IP) $(PORT) $(BOOTSTRAP_IP) $(BOOTSTRAP_PORT); \
	else \
		java -cp $(OUT_DIR) Main $(IP) $(PORT); \
	fi

clean:
	rm -rf $(OUT_DIR)
