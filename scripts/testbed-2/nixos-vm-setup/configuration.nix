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
    wget curl mtr htop iproute2 pciutils rsync tree
    zsh grml-zsh-config
    git
    unixtools.ping
    python3
    iperf2
  ];
  # environment.variables = {
  # };

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
      #
      # In current grml zshrc, changing `$PROMPT` no longer works,
      # and `zstyle` is used instead, see:
      # https://unix.stackexchange.com/questions/656152/why-does-setting-prompt-have-no-effect-in-grmls-zshrc

      # Disable the grml `sad-smiley` on the right for exit codes != 0;
      # it makes copy-pasting out terminal output difficult.
      # Done by setting the `items` of the right-side setup to the empty list
      # (as of writing, the default is `items sad-smiley`).
      # See: https://bts.grml.org/grml/issue2267
      zstyle ':prompt:grml:right:setup' items

      # Add nix-shell indicator that makes clear when we're in nix-shell.
      # Set the prompt items to include it in addition to the defaults:
      # Described in: http://bewatermyfriend.org/p/2013/003/
      function nix_shell_prompt () {
        REPLY=''${IN_NIX_SHELL+"(nix-shell):"}
      }
      grml_theme_add_token nix-shell-indicator -f nix_shell_prompt '%F{magenta}' '%f'
      zstyle ':prompt:grml:left:setup' items rc nix-shell-indicator change-root user at host path vcs percent
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

  # This value determines the NixOS release from which the default
  # settings for stateful data, like file locations and database versions
  # on your system were taken. It‘s perfectly fine and recommended to leave
  # this value at the release version of the first install of this system.
  # Before changing this value read the documentation for this option
  # (e.g. man configuration.nix or on https://nixos.org/nixos/options.html).
  system.stateVersion = "21.11"; # Did you read the comment?

}
