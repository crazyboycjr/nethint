{
  inputs.nixpkgs.url = "github:nixos/nixpkgs?rev=fb45fa64ae3460d6bd2701ab5a6c4512d781f166";
  outputs = { self, nixpkgs }: {
    nixosConfigurations.nixos = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        (
          { pkgs, ... }: {
            nix.registry.nixpkgs = {
              from = { type = "indirect"; id = "nixpkgs"; };
              flake = nixpkgs;
            };
          }
        )
        ./configuration.nix
      ];
    };
  };
}
