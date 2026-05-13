{
  description = "Noxu DB — Rust port of BerkeleyDB Java Edition with replication";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachSystem [
      "x86_64-linux"
      "aarch64-linux"
      "x86_64-darwin"
      "aarch64-darwin"
    ] (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Read the channel from rust-toolchain.toml so flake and toolchain file
        # stay in sync automatically.
        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;

        # Cross-compilation targets to install as additional rustup targets.
        # These enable `cargo build --target <triple>` without a separate rustup.
        crossTargets = [
          "aarch64-unknown-linux-gnu"      # AWS Graviton, Raspberry Pi 4
          "armv7-unknown-linux-gnueabihf"  # 32-bit ARM (Raspberry Pi 2/3)
          "riscv64gc-unknown-linux-gnu"    # RISC-V 64-bit
        ];

        # The Rust toolchain with cross-compilation targets pre-installed.
        rustWithTargets = toolchain.override {
          extensions = [ "rust-src" "llvm-tools-preview" ];
          targets = crossTargets;
        };

        # Cross-linkers for non-native targets.
        crossLinkers = with pkgs; [
          pkgsCross.aarch64-multiplatform.buildPackages.gcc
          pkgsCross.armv7l-hf-multiplatform.buildPackages.gcc
          pkgsCross.riscv64.buildPackages.gcc
        ];

        # macOS framework deps (required by quinn / rustls on Darwin).
        darwinFrameworks = with pkgs.darwin.apple_sdk.frameworks; pkgs.lib.optionals
          pkgs.stdenv.isDarwin
          [ Security SystemConfiguration CoreFoundation ];

        # Linux-only network tools needed for chaos / netem tests.
        linuxNetTools = pkgs.lib.optionals pkgs.stdenv.isLinux (with pkgs; [
          iproute2   # provides `tc` for netem fault injection
        ]);

        # Development shell dependencies shared across all platforms.
        commonDeps = with pkgs; [
          rustWithTargets

          # Build tools
          pkg-config
          openssl.dev

          # Test / bench helpers
          cargo-nextest
          cargo-criterion
          hyperfine

          # Formatting / lint
          nixpkgs-fmt

          # Debug
          gdb
        ] ++ linuxNetTools ++ darwinFrameworks;

      in {
        # `nix develop` — drops into a full dev shell.
        devShells.default = pkgs.mkShell {
          name = "noxu-db-dev";
          buildInputs = commonDeps;

          # Cargo config: point cross-compilation linkers so that
          # `cargo build --target <triple>` works out of the box.
          CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER =
            "${pkgs.pkgsCross.aarch64-multiplatform.buildPackages.gcc}/bin/aarch64-unknown-linux-gnu-gcc";
          CARGO_TARGET_ARMV7_UNKNOWN_LINUX_GNUEABIHF_LINKER =
            "${pkgs.pkgsCross.armv7l-hf-multiplatform.buildPackages.gcc}/bin/arm-unknown-linux-gnueabihf-gcc";
          CARGO_TARGET_RISCV64GC_UNKNOWN_LINUX_GNU_LINKER =
            "${pkgs.pkgsCross.riscv64.buildPackages.gcc}/bin/riscv64-unknown-linux-gnu-gcc";

          shellHook = ''
            echo "Noxu DB dev shell — Rust $(rustc --version)"
            echo "  cross targets: ${pkgs.lib.concatStringsSep ", " crossTargets}"
            echo "  tc netem: $(which tc 2>/dev/null && echo available || echo not available)"
          '';
        };

        # `nix develop .#ci` — minimal shell for CI (no GUI tools, no cross-linkers).
        devShells.ci = pkgs.mkShell {
          name = "noxu-db-ci";
          buildInputs = with pkgs; [
            rustWithTargets
            pkg-config
            openssl.dev
            cargo-nextest
          ] ++ linuxNetTools ++ darwinFrameworks;
        };
      }
    );
}
