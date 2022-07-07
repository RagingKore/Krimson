ARG BUILDER_IMG=mcr.microsoft.com/dotnet/sdk:6.0

FROM $BUILDER_IMG AS build

ENV TEST_PROJECT "tests/Krimson.Tests/Krimson.Tests.csproj"

ENV DOTNET_CLI_TELEMETRY_OPTOUT 1
ENV DOTNET_SKIP_FIRST_TIME_EXPERIENCE 1
ENV DOTNET_NOLOGO true
ENV DOTNET_TIEREDPGO 1 # enable dynamic profile-guided optimization

USER root

WORKDIR /code 

COPY .git .git
COPY *.sln *.props *.targets ./

COPY /src/*.props /src/*.targets /src/*/*.csproj src/
RUN for file in $(ls src/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

COPY /tests/*.props /tests/*.targets /tests/*/*.csproj tests/
RUN for file in $(ls tests/*.csproj); do mkdir -p ./${file%.*}/ && mv $file ./${file%.*}/; done

RUN dotnet restore $TEST_PROJECT --packages packages --verbosity minimal 

COPY src src
COPY tests tests

RUN dotnet build $TEST_PROJECT -c Release --no-restore

##---------------------------------------------
## Publish application
##---------------------------------------------
#FROM build AS publish
#ARG APP_DIR
#RUN dotnet publish src/${TEST_PROJECT} -c Release -o /app/publish --no-build /p:SatelliteResourceLanguages=en
#
##---------------------------------------------
## Create final runtime image
##---------------------------------------------
#FROM runner AS app
#COPY --from=publish /app/publish .

##---------------------------------------------
## Datadog
##---------------------------------------------
#
#ENV DD_SITE "datadoghq.eu"
#ENV DD_API_KEY 46320a9f03f00e82c9b63e911322b81c
#ENV DD_ENV "development"
#ENV DD_SERVICE "krimson"
#ENV DD_VERSION "1.1.0-alpha.0.7"
#ENV DD_LOGS_INJECTION true
#ENV DD_TRACE_SAMPLE_RATE "1"
#ENV DD_OTLP_CONFIG_TRACES_SPAN_NAME_AS_RESOURCE_NAME true
#ENV DD_TRACE_DEBUG true
#ENV DD_RUNTIME_METRICS_ENABLED true
#
## Install datadog agent
#RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* 
#
## Download and install the Tracer
#RUN mkdir -p /opt/datadog && mkdir -p /var/log/datadog \
#    && TRACER_VERSION=$(curl -s https://api.github.com/repos/DataDog/dd-trace-dotnet/releases/latest | grep tag_name | cut -d '"' -f 4 | cut -c2-) \
#    && curl -LO https://github.com/DataDog/dd-trace-dotnet/releases/download/v${TRACER_VERSION}/datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb \
#    && dpkg -i ./datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb \
#    && rm ./datadog-dotnet-apm_${TRACER_VERSION}_amd64.deb
#
## Enable the tracer
#ENV CORECLR_ENABLE_PROFILING=1
#ENV CORECLR_PROFILER={846F5F1C-F9AE-4B07-969E-05C26BC060D8}
#ENV CORECLR_PROFILER_PATH=/opt/datadog/Datadog.Trace.ClrProfiler.Native.so
#ENV DD_DOTNET_TRACER_HOME=/opt/datadog
#ENV DD_INTEGRATIONS=/opt/datadog/integrations.json
#    
#RUN DD_AGENT_MAJOR_VERSION=7 DD_API_KEY=46320a9f03f00e82c9b63e911322b81c DD_SITE="datadoghq.eu" DD_INSTALL_ONLY=true bash -c "$(curl -L https://s3.amazonaws.com/dd-agent/scripts/install_script.sh)"
#
#ENTRYPOINT service datadog-agent start & dotnet test $TEST_PROJECT -c Release --no-build --blame-hang-timeout 3min --logger:"junit;MethodFormat=Full;FailureBodyFormat=Verbose;LogFilePath=../../test-results/{assembly}.junit.xml"

ENV DD_ENV "development"
ENV DD_SERVICE "krimson"

ENTRYPOINT dotnet test $TEST_PROJECT -c Release --no-build --blame-hang-timeout 3min --logger:"junit;MethodFormat=Full;FailureBodyFormat=Verbose;LogFilePath=../../test-results/{assembly}.junit.xml"