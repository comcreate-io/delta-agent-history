{
  description = "Harness-independent coding session history through Delta";
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
          pname = "delta-agent-history";
          version = "0.2.0";
          src = self;
          pyproject = true;
          build-system = [ pkgs.python3Packages.setuptools ];
          nativeCheckInputs = [
            pkgs.git
            pkgs.nodejs
          ];
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
            "delta_history.core"
            "delta_history.cli"
            "delta_history.mcp"
          ];
          postFixup = ''
            wrapProgram "$out/bin/t3-delta" --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.git ]}
            wrapProgram "$out/bin/delta-history" --prefix PATH : ${pkgs.lib.makeBinPath [ pkgs.git ]}
          '';
          meta = {
            description = "Harness-independent private coding session history in Delta";
            homepage = "https://github.com/comcreate-io/delta-agent-history";
            license = pkgs.lib.licenses.mit;
            platforms = systems;
            mainProgram = "delta-history";
          };
        };
    in
    {
      packages = forAllSystems (system: {
        default = packageFor system;
        t3-delta-recorder = packageFor system;
        delta-agent-history = packageFor system;
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
              pkgs.nodejs
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
