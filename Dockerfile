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

ENTRYPOINT dotnet test $TEST_PROJECT -c Release --no-build --blame-hang-timeout 3min --logger:"junit;MethodFormat=Full;FailureBodyFormat=Verbose;LogFilePath=../../test-results/{assembly}.junit.xml"