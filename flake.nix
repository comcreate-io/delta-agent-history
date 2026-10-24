{
  description = "Portable private T3 Code to Delta recorder";
  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  outputs =
    { self, nixpkgs }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      packageFor =
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        pkgs.python3Packages.buildPythonApplication {
          pname = "t3-delta-recorder";
          version = "0.1.0";
          src = self;
          pyproject = true;
          build-system = [ pkgs.python3Packages.setuptools ];
          nativeCheckInputs = [ pkgs.git ];
          nativeBuildInputs = [ pkgs.makeWrapper ];
          doCheck = true;
          checkPhase = ''
            runHook preCheck
            export PYTHONDONTWRITEBYTECODE=1
            python -m unittest discover -s tests -v
            runHook postCheck
          '';
          pythonImportsCheck = [
            "t3_delta"
            "t3_delta.cli"
            "t3_delta.services"
          ];
          postFixup = ''
            wrapProgram "$out/bin/t3-delta" --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.git ]}
          '';
          meta = {
            description = "Private prospective T3 Code conversation recording in Delta";
            homepage = "https://github.com/comcreate-io/t3-delta-recorder";
            license = pkgs.lib.licenses.mit;
            platforms = systems;
            mainProgram = "t3-delta";
          };
        };
    in
    {
      packages = forAllSystems (system: {
        default = packageFor system;
        t3-delta-recorder = packageFor system;
      });
      checks = forAllSystems (system: {
        recorder = packageFor system;
      });
      devShells = forAllSystems (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
        in
        {
          default = pkgs.mkShell {
            packages = [
              pkgs.git
              (pkgs.python3.withPackages (ps: [
                ps.build
                ps.setuptools
                ps.wheel
                ps.pip
              ]))
            ];
          };
        }
      );
    };
}
