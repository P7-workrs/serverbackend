using Xunit;
using WorkrsBackend;
using Moq;
using WorkrsBackend.Config;
using WorkrsBackend.DataHandling;
using WorkrsBackend.RabbitMQ;
using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using WorkrsBackend.DTOs;
using System.Text.Json;
using System.Reflection;
using FluentFTP;
using Newtonsoft.Json.Linq;
using Microsoft.AspNetCore.Http;
using Serilog;
using System.Text;
using System.Threading;

namespace BackendTestProject
{
    public class ServiceLogicTests
    {
        private readonly ServiceLogic _sut;
        private readonly Mock<IServerConfig> _ServerConfigMock;
        private readonly Mock<ISharedResourceHandler> _SharedResourceHandlerMock;
        private readonly Mock<IRabbitMQHandler> _RabbitMQHandlerMock;
        public ServiceLogicTests()
        {
            _ServerConfigMock = new Mock<IServerConfig>();
            _SharedResourceHandlerMock = new Mock<ISharedResourceHandler>();
            _RabbitMQHandlerMock = new Mock<IRabbitMQHandler>();
            _sut = new ServiceLogic(_ServerConfigMock.Object, _SharedResourceHandlerMock.Object, _RabbitMQHandlerMock.Object);
        }

        [Fact]

        public void StartAsync_KnownConnection_ExpectConnectionReceived()
        {



        }
    }
}