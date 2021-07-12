# Edit this configuration file to define what should be installed on
# your system.  Help is available in the configuration.nix(5) man page
# and in the NixOS manual (accessible by running ‘nixos-help’).

{ config, pkgs, lib, modulesPath, ... }:
let
  sshPublicKeys = import (./. + "/pubkeys.nix");
in
{
  imports =
    [ # Include the results of the hardware scan.
      ./hardware-configuration.nix
    ];

  # Use the GRUB 2 boot loader.
  boot.loader.grub.enable = true;
  boot.loader.grub.version = 2;
  boot.loader.grub.efiSupport = false;
  # boot.loader.grub.efiInstallAsRemovable = true;
  # boot.loader.efi.efiSysMountPoint = "/boot/efi";
  # Define on which hard drive you want to install Grub.
  boot.loader.grub.device = "/dev/sda"; # or "nodev" for efi only
  boot.loader.grub.useOSProber = false;
  boot.loader.timeout = 0;
  boot.kernelParams = [
    "console=tty1"
    "console=ttyS0,115200n8"
    "nosplash"
  ];
  boot.loader.grub.extraConfig = ''
    serial --unit=0 --speed=115200 --word=8 --parity=no --stop=1
    terminal_input --append serial
    terminal_output --append serial
  '';

  boot.tmpOnTmpfs = true;
  boot.initrd.checkJournalingFS = false;

  networking.hostName = "nixos"; # Define your hostname.
  networking.wireless.enable = false;  # Enables wireless support via wpa_supplicant.
  networking.iproute2.enable = true;
  # networking.nameservers = [
  #   "152.3.140.31"
  #   "152.3.140.1"
  # ];

  # Set your time zone.
  time.timeZone = "America/New_York";

  # The global useDHCP flag is deprecated, therefore explicitly set to false here.
  # Per-interface useDHCP will be mandatory in the future, so this generated config
  # replicates the default behaviour.
  networking.useDHCP = false;
  networking.interfaces.ens2.useDHCP = true;

  # the following setting does not work well, so setup this interface later
  # networking.interfaces.ens3 = {
  #   mtu = 9000;
  #   useDHCP = false;
  #   ipv4.addresses = [
  #     {
  #       address = "192.168.211.3";
  #       prefixLength = 24;
  #     }
  #   ];
  # };

  # Configure network proxy if necessary
  # networking.proxy.default = "http://user:password@proxy:port/";
  # networking.proxy.noProxy = "127.0.0.1,localhost,internal.domain";

  # Select internationalisation properties.
  i18n.defaultLocale = "en_US.UTF-8";
  # i18n.supportedLocales = [
  #   "all"
  # ];
  # console = {
  #   font = "Lat2-Terminus16";
  #   keyMap = "us";
  # };

  users.defaultUserShell = pkgs.zsh;

  # Define a user account. Don't forget to set a password with ‘passwd’.
  users.users.tenant = {
    isNormalUser = true;
    initialHashedPassword = "$6$/1p.mph/$D7ABM0Q92dPIUZh8YPc0y/3D6ioCRR4kZGQuGpq9iwMzaolKL7nbY.jzI3dfwQ9qVl50w2CujpEtraXSdpjDk.";
    extraGroups = [ "wheel" ]; # Enable ‘sudo’ for the user.
    openssh.authorizedKeys.keys = sshPublicKeys;
  };
  users.extraUsers.root = {
    initialHashedPassword = "$6$/1p.mph/$D7ABM0Q92dPIUZh8YPc0y/3D6ioCRR4kZGQuGpq9iwMzaolKL7nbY.jzI3dfwQ9qVl50w2CujpEtraXSdpjDk.";
    openssh.authorizedKeys.keys = sshPublicKeys;
  };

  security.sudo.extraConfig = ''
    tenant ALL=(ALL) NOPASSWD:ALL
  '';

  # List packages installed in system profile. To search, run:
  # $ nix search wget
  environment.systemPackages = with pkgs; [
    vim
    wget curl mtr htop iproute2 pciutils rsync tree jq
    zsh grml-zsh-config
    git
    unixtools.ping
    python3
    iperf2
    patchelf binutils
  ];
  # environment.variables = {
  # };
  environment.etc."../home/tenant/.zshrc" = {
    user = "tenant";
    group = "users";
    mode = "0644";
    text = "# Created automatically";
  };

  # Some programs need SUID wrappers, can be configured further or are
  # started in user sessions.
  programs.mtr.enable = true;
  programs.gnupg.agent = {
    enable = true;
    enableSSHSupport = true;
  };
  programs.vim.defaultEditor = true;
  programs.zsh = {
    enable = true;
    autosuggestions.enable = true;
    enableCompletion = true;
    syntaxHighlighting.enable = true;
    promptInit = ""; # otherwise it'll override the grml prompt
    interactiveShellInit = ''
      # Note that loading grml's zshrc here will override NixOS settings such as
      # `programs.zsh.histSize`, so they will have to be set again below.
      source ${pkgs.grml-zsh-config}/etc/zsh/zshrc

      # Increase history size.
      HISTSIZE=10000000
      HISTFILESIZE=400000000

      # Prompt modifications.
      # https://discourse.nixos.org/t/using-zsh-with-grml-config-and-nix-shell-prompt-indicator/13838
      zstyle ':prompt:grml:right:setup' items

      function nix_shell_prompt () {
        if echo "$PATH" | ${pkgs.gnugrep}/bin/grep -qc '/nix/store'; then
          IN_NIX_SHELL=1
        fi
        REPLY=''${IN_NIX_SHELL+"(nix-shell):"}
      }
      grml_theme_has_token nix-shell-indicator || grml_theme_add_token nix-shell-indicator -f nix_shell_prompt '%F{magenta}' '%f'
      zstyle ':prompt:grml:left:setup' items rc nix-shell-indicator change-root user at host path vcs percent

      # fix zsh completion for nix experimental features
      function _nix() {
        local ifs_bk="$IFS"
        local input=("''${(Q)words[@]}")
        IFS=$'\n'
        local res=($(NIX_GET_COMPLETIONS=$((CURRENT - 1)) "$input[@]"))
        IFS="$ifs_bk"
        local tpe="''${''${res[1]}%%>	*}" # \t* here
        local -a suggestions
        declare -a suggestions
        for suggestion in ''${res:1}; do
          # FIXME: This doesn't work properly if the suggestion word contains a `:`
          # itself
          suggestions+="''${suggestion/	/:}" # \t here
        done
        if [[ "$tpe" == filenames ]]; then
          compadd -f
        fi
        _describe 'nix' suggestions
      }

      compdef _nix nix

      setopt noextendedglob
    '';
  };

  # List services that you want to enable:

  # Enable the OpenSSH daemon.
  services.openssh.enable = true;

  # Open ports in the firewall.
  # networking.firewall.allowedTCPPorts = [ ... ];
  # networking.firewall.allowedUDPPorts = [ ... ];
  # Or disable the firewall altogether.
  networking.firewall.enable = false;

  # Nixpkgs
  nixpkgs.config.allowUnfree = true;

  system.activationScripts = {
    ldlinux.text = ''
      mkdir -m 0755 -p /lib64 /lib
      linker=ld-linux-x86-64.so.2
      ln -sfn "${pkgs.glibc}/lib/$linker" /lib64/.$linker.tmp
      mv /lib64/.$linker.tmp /lib64/$linker
      ln -sfn "${pkgs.glibc}/lib/$linker" /lib/.$linker.tmp
      mv /lib/.$linker.tmp /lib/$linker
    '';

    binbash.text = ''
      mkdir -m 0755 -p /bin
      ln -sfn "${pkgs.bashInteractive}/bin/bash" /bin/.bash.tmp
      mv /bin/.bash.tmp /bin/bash # atomically replace /bin/bash
    '';
  };

  nix = {
    package = pkgs.nixUnstable;
    extraOptions = ''
      experimental-features = nix-command flakes ca-references
    '';
  };

  # This value determines the NixOS release from which the default
  # settings for stateful data, like file locations and database versions
  # on your system were taken. It‘s perfectly fine and recommended to leave
  # this value at the release version of the first install of this system.
  # Before changing this value read the documentation for this option
  # (e.g. man configuration.nix or on https://nixos.org/nixos/options.html).
  system.stateVersion = "21.11"; # Did you read the comment?

}
