{
  # Tabbify supervisor node — NixOS module as a flake.
  #
  # The NixOS-idiomatic "one command" (the curl|sh installer is for
  # FHS distros; NixOS gets the declarative path):
  #
  #   # flake-based system configuration:
  #   inputs.tabbify.url = "github:tabbify-io/tabbify-service-supervisor";
  #   ...
  #   imports = [ tabbify.nixosModules.node ];
  #
  #   # or ad-hoc from a non-flake configuration.nix:
  #   imports = [
  #     (builtins.getFlake "github:tabbify-io/tabbify-service-supervisor").nixosModules.node
  #   ];
  #
  # then `sudo nixos-rebuild switch`. The module is self-contained
  # (binaries come from the public release bucket at runtime, never the
  # Nix store — see nixos/tabbify-node.nix for the OTA design).
  description = "Tabbify supervisor node (mesh built in) — NixOS module";

  outputs = { self }: {
    nixosModules.node = import ./nixos/tabbify-node.nix;
    nixosModules.default = self.nixosModules.node;
  };
}
