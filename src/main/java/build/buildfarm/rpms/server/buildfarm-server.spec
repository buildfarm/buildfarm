Name:           buildfarm-server
Version:        1.10.0
Release:        0
Summary:        Buildfarm Server
URL:            https://github.com/buildfarm/buildfarm

License:        Apache License, v2.0

Requires:       java-11-openjdk-headless
BuildRequires:  systemd
%{?systemd_requires}

%description
A Buildfarm server with an instance can be used strictly as an ActionCache and
ContentAddressableStorage to improve build performance.

%define debug_package %{nil}
%define __jar_repack %{nil}

%prep

%build

%install
app_dir=%{buildroot}/usr/local/buildfarm-server
config_dir=%{buildroot}/%{_sysconfdir}/buildfarm-server/config
log_dir=%{buildroot}/var/log/buildfarm-server
service_dir=%{buildroot}/%{_unitdir}

mkdir -p $app_dir
cp {buildfarm-server_deploy.jar} $app_dir
cp {logging.properties} $app_dir
mkdir -p $config_dir
cp {config.yml} $config_dir/server.config
mkdir -p $log_dir
mkdir -p $service_dir
cp {buildfarm-server.service} $service_dir/%{name}.service

%files
/usr/local/buildfarm-server/buildfarm-server_deploy.jar
%attr(644, root, root) /usr/local/buildfarm-server/logging.properties
%attr(644, root, root) %{_unitdir}/%{name}.service
%attr(644, root, root) %{_sysconfdir}/buildfarm-server/config/server.config
%dir %attr(755, root, root)  /var/log/buildfarm-server

%post
%systemd_post %{name}.service
%{_bindir}/systemctl enable %{name}.service
%{_bindir}/systemctl start %{name}.service

%preun
%systemd_preun %{name}.service

%postun
rm -rf /etc/buildfarm-server
rm -rf /usr/local/buildfarm-server
rm -rf /var/log/buildfarm-server

%systemd_postun_with_restart %{name}.service
