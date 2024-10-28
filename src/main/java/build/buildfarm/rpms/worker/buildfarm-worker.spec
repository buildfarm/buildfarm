Name:           buildfarm-worker
Version:        1.10.0
Release:        0
Summary:        Buildfarm Worker
URL:            https://github.com/buildfarm/buildfarm

License:        Apache License, v2.0

Requires:       java-11-openjdk-headless
BuildRequires:  systemd
%{?systemd_requires}

%description
Workers of all types throughout buildfarm are responsible for presenting execution
roots to operations that they are matched with, fetching content from a CAS, executing
those processes, and reporting the outputs and results of executions.

%define debug_package %{nil}
%define __jar_repack %{nil}

%prep

%build

%install
app_dir=%{buildroot}/usr/local/buildfarm-worker
config_dir=%{buildroot}/%{_sysconfdir}/buildfarm-worker/config
log_dir=%{buildroot}/var/log/buildfarm-worker
service_dir=%{buildroot}/%{_unitdir}

mkdir -p $app_dir
cp {buildfarm-shard-worker_deploy.jar} $app_dir
cp {logging.properties} $app_dir
mkdir -p $config_dir
cp {config.yml} $config_dir/worker.config
mkdir -p $log_dir
mkdir -p $service_dir
cp {buildfarm-worker.service} $service_dir/%{name}.service

%files
/usr/local/buildfarm-worker/buildfarm-shard-worker_deploy.jar
%attr(644, root, root) /usr/local/buildfarm-worker/logging.properties
%attr(644, root, root) %{_unitdir}/%{name}.service
%attr(644, root, root) %{_sysconfdir}/buildfarm-worker/config/worker.config
%dir %attr(755, root, root)  /var/log/buildfarm-worker

%post
%systemd_post %{name}.service
%{_bindir}/systemctl enable %{name}.service
%{_bindir}/systemctl start %{name}.service

%preun
%systemd_preun %{name}.service

%postun
rm -rf /usr/local/buildfarm-worker
rm -rf /etc/buildfarm-worker
rm -rf /var/log/buildfarm-worker

%systemd_postun_with_restart %{name}.service
