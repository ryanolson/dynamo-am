.PHONY: test test-all test-doc clean

# Default test target - skips documentation tests
test:
	cargo test -- --skip docs

# Run all tests including documentation tests
test-all:
	cargo test

# Run only documentation tests
test-doc:
	cargo test --doc

# Clean build artifacts
clean:
	cargo clean

