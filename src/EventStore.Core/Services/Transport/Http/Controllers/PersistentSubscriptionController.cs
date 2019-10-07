using System;
using System.Collections.Generic;
using System.Linq;
using EventStore.Common.Log;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Core.Messaging;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using EventStore.Transport.Http.EntityManagement;
using ClientMessages = EventStore.Core.Messages.ClientMessage.PersistentSubscriptionNackEvents;
using EventStore.Core.Services.PersistentSubscription;
using EventStore.Core.Data;
using EventStore.Common.Utils;
using EventStore.Transport.Http.Atom;

namespace EventStore.Core.Services.Transport.Http.Controllers {
	public class PersistentSubscriptionController : CommunicationController {
		private readonly IHttpForwarder _httpForwarder;
		private readonly IPublisher _networkSendQueue;
		private const int DefaultNumberOfMessagesToGet = 1;
		private static readonly ICodec[] DefaultCodecs = {Codec.Json, Codec.Xml};

		private static readonly ICodec[] AtomCodecs = {
			Codec.CompetingXml,
			Codec.CompetingJson,
		};

		private static readonly ILogger Log = LogManager.GetLoggerFor<PersistentSubscriptionController>();

		public PersistentSubscriptionController(IHttpForwarder httpForwarder, IPublisher publisher,
			IPublisher networkSendQueue)
			: base(publisher) {
			_httpForwarder = httpForwarder;
			_networkSendQueue = networkSendQueue;
		}

		protected override void SubscribeCore(IHttpService service) {
			Register(service, "/subscriptions", HttpMethod.Get, GetAllSubscriptionInfo, Codec.NoCodecs, DefaultCodecs, AuthorizationLevel.User);
			Register(service, "/subscriptions/{stream}", HttpMethod.Get, GetSubscriptionInfoForStream, Codec.NoCodecs,
				DefaultCodecs, AuthorizationLevel.User);
			Register(service, "/subscriptions/{stream}/{subscription}", HttpMethod.Put, PutSubscription, DefaultCodecs,
				DefaultCodecs, AuthorizationLevel.Ops);
			Register(service, "/subscriptions/{stream}/{subscription}", HttpMethod.Post, PostSubscription,
				DefaultCodecs, DefaultCodecs, AuthorizationLevel.Ops);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}", HttpMethod.Delete, AuthorizationLevel.Ops, DeleteSubscription);
			Register(service, "/subscriptions/{stream}/{subscription}", HttpMethod.Get, GetNextNMessages,
				Codec.NoCodecs, AtomCodecs, AuthorizationLevel.User);
			Register(service, "/subscriptions/{stream}/{subscription}?embed={embed}", HttpMethod.Get, GetNextNMessages,
				Codec.NoCodecs, AtomCodecs, AuthorizationLevel.User);
			Register(service, "/subscriptions/{stream}/{subscription}/{count}?embed={embed}", HttpMethod.Get,
				GetNextNMessages, Codec.NoCodecs, AtomCodecs, AuthorizationLevel.User);
			Register(service, "/subscriptions/{stream}/{subscription}/info", HttpMethod.Get, GetSubscriptionInfo,
				Codec.NoCodecs, DefaultCodecs, AuthorizationLevel.User);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}/ack/{messageid}", HttpMethod.Post, AuthorizationLevel.User,
				AckMessage);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}/nack/{messageid}?action={action}",
				HttpMethod.Post, AuthorizationLevel.User, NackMessage);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}/ack?ids={messageids}", HttpMethod.Post, AuthorizationLevel.User,
				AckMessages);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}/nack?ids={messageids}&action={action}",
				HttpMethod.Post, AuthorizationLevel.User, NackMessages);
			RegisterUrlBased(service, "/subscriptions/{stream}/{subscription}/replayParked", HttpMethod.Post,
				AuthorizationLevel.User, ReplayParkedMessages);
		}


		private void AckMessages(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void NackMessages(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private static string BuildSubscriptionGroupKey(string stream, string groupName) {
			return stream + "::" + groupName;
		}

		private void AckMessage(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void NackMessage(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void ReplayParkedMessages(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void PutSubscription(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void PostSubscription(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private SubscriptionConfigData ParseConfig(SubscriptionConfigData config) {
			if (config == null) {
				return new SubscriptionConfigData();
			}

			return new SubscriptionConfigData {
				ResolveLinktos = config.ResolveLinktos,
				StartFrom = config.StartFrom,
				MessageTimeoutMilliseconds = config.MessageTimeoutMilliseconds,
				ExtraStatistics = config.ExtraStatistics,
				MaxRetryCount = config.MaxRetryCount,
				BufferSize = config.BufferSize,
				LiveBufferSize = config.LiveBufferSize,
				ReadBatchSize = config.ReadBatchSize,
				CheckPointAfterMilliseconds = config.CheckPointAfterMilliseconds,
				MinCheckPointCount = config.MinCheckPointCount,
				MaxCheckPointCount = config.MaxCheckPointCount,
				MaxSubscriberCount = config.MaxSubscriberCount
			};
		}

		private bool ValidateConfig(SubscriptionConfigData config, HttpEntityManager http) {
			if (config.BufferSize <= 0) {
				SendBadRequest(
					http,
					string.Format(
						"Buffer Size ({0}) must be positive",
						config.BufferSize));
				return false;
			}

			if (config.LiveBufferSize <= 0) {
				SendBadRequest(
					http,
					string.Format(
						"Live Buffer Size ({0}) must be positive",
						config.LiveBufferSize));
				return false;
			}

			if (config.ReadBatchSize <= 0) {
				SendBadRequest(
					http,
					string.Format(
						"Read Batch Size ({0}) must be positive",
						config.ReadBatchSize));
				return false;
			}

			if (!(config.BufferSize > config.ReadBatchSize)) {
				SendBadRequest(
					http,
					string.Format(
						"BufferSize ({0}) must be larger than ReadBatchSize ({1})",
						config.BufferSize, config.ReadBatchSize));
				return false;
			}

			return true;
		}

		private static string CalculateNamedConsumerStrategyForOldClients(SubscriptionConfigData data) {
			var namedConsumerStrategy = data == null ? null : data.NamedConsumerStrategy;
			if (string.IsNullOrEmpty(namedConsumerStrategy)) {
				var preferRoundRobin = data == null || data.PreferRoundRobin;
				namedConsumerStrategy = preferRoundRobin
					? SystemConsumerStrategies.RoundRobin
					: SystemConsumerStrategies.DispatchToSingle;
			}

			return namedConsumerStrategy;
		}

		private void DeleteSubscription(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void GetAllSubscriptionInfo(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
			var envelope = new SendToHttpEnvelope(
				_networkSendQueue, http,
				(args, message) =>
					http.ResponseCodec.To(ToSummaryDto(http,
						message as MonitoringMessage.GetPersistentSubscriptionStatsCompleted).ToArray()),
				(args, message) => StatsConfiguration(http, message));
			var cmd = new MonitoringMessage.GetAllPersistentSubscriptionStats(envelope);
			Publish(cmd);
		}

		private void GetSubscriptionInfoForStream(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private void GetSubscriptionInfo(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		private static ResponseConfiguration StatsConfiguration(HttpEntityManager http, Message message) {
			int code;
			var m = message as MonitoringMessage.GetPersistentSubscriptionStatsCompleted;
			if (m == null) throw new Exception("unexpected message " + message);
			switch (m.Result) {
				case MonitoringMessage.GetPersistentSubscriptionStatsCompleted.OperationStatus.Success:
					code = HttpStatusCode.OK;
					break;
				case MonitoringMessage.GetPersistentSubscriptionStatsCompleted.OperationStatus.NotFound:
					code = HttpStatusCode.NotFound;
					break;
				case MonitoringMessage.GetPersistentSubscriptionStatsCompleted.OperationStatus.NotReady:
					code = HttpStatusCode.ServiceUnavailable;
					break;
				default:
					code = HttpStatusCode.InternalServerError;
					break;
			}

			return new ResponseConfiguration(code, http.ResponseCodec.ContentType,
				http.ResponseCodec.Encoding);
		}

		private void GetNextNMessages(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

		}

		string parkedMessageUriTemplate =
			"/streams/" + Uri.EscapeDataString("$persistentsubscription") + "-{0}::{1}-parked";

		private IEnumerable<SubscriptionInfo> ToDto(HttpEntityManager manager,
			MonitoringMessage.GetPersistentSubscriptionStatsCompleted message) {
			if (message == null) yield break;
			if (message.SubscriptionStats == null) yield break;

			foreach (var stat in message.SubscriptionStats) {
				string escapedStreamId = Uri.EscapeDataString(stat.EventStreamId);
				string escapedGroupName = Uri.EscapeDataString(stat.GroupName);
				var info = new SubscriptionInfo {
					Links = new List<RelLink>() {
						new RelLink(
							MakeUrl(manager,
								string.Format("/subscriptions/{0}/{1}/info", escapedStreamId, escapedGroupName)),
							"detail"),
						new RelLink(
							MakeUrl(manager,
								string.Format("/subscriptions/{0}/{1}/replayParked", escapedStreamId,
									escapedGroupName)), "replayParked")
					},
					EventStreamId = stat.EventStreamId,
					GroupName = stat.GroupName,
					Status = stat.Status,
					AverageItemsPerSecond = stat.AveragePerSecond,
					TotalItemsProcessed = stat.TotalItems,
					CountSinceLastMeasurement = stat.CountSinceLastMeasurement,
					LastKnownEventNumber = stat.LastKnownMessage,
					LastProcessedEventNumber = stat.LastProcessedEventNumber,
					ReadBufferCount = stat.ReadBufferCount,
					LiveBufferCount = stat.LiveBufferCount,
					RetryBufferCount = stat.RetryBufferCount,
					TotalInFlightMessages = stat.TotalInFlightMessages,
					OutstandingMessagesCount = stat.OutstandingMessagesCount,
					ParkedMessageUri = MakeUrl(manager,
						string.Format(parkedMessageUriTemplate, escapedStreamId, escapedGroupName)),
					GetMessagesUri = MakeUrl(manager,
						string.Format("/subscriptions/{0}/{1}/{2}", escapedStreamId, escapedGroupName,
							DefaultNumberOfMessagesToGet)),
					Config = new SubscriptionConfigData {
						CheckPointAfterMilliseconds = stat.CheckPointAfterMilliseconds,
						BufferSize = stat.BufferSize,
						LiveBufferSize = stat.LiveBufferSize,
						MaxCheckPointCount = stat.MaxCheckPointCount,
						MaxRetryCount = stat.MaxRetryCount,
						MessageTimeoutMilliseconds = stat.MessageTimeoutMilliseconds,
						MinCheckPointCount = stat.MinCheckPointCount,
						NamedConsumerStrategy = stat.NamedConsumerStrategy,
						PreferRoundRobin = stat.NamedConsumerStrategy == SystemConsumerStrategies.RoundRobin,
						ReadBatchSize = stat.ReadBatchSize,
						ResolveLinktos = stat.ResolveLinktos,
						StartFrom = stat.StartFrom,
						ExtraStatistics = stat.ExtraStatistics,
						MaxSubscriberCount = stat.MaxSubscriberCount
					},
					Connections = new List<ConnectionInfo>()
				};
				if (stat.Connections != null) {
					foreach (var connection in stat.Connections) {
						info.Connections.Add(new ConnectionInfo {
							Username = connection.Username,
							From = connection.From,
							AverageItemsPerSecond = connection.AverageItemsPerSecond,
							CountSinceLastMeasurement = connection.CountSinceLastMeasurement,
							TotalItemsProcessed = connection.TotalItems,
							AvailableSlots = connection.AvailableSlots,
							InFlightMessages = connection.InFlightMessages,
							ExtraStatistics = connection.ObservedMeasurements ?? new List<Measurement>(),
							ConnectionName = connection.ConnectionName,
						});
					}
				}

				yield return info;
			}
		}

		private IEnumerable<SubscriptionSummary> ToSummaryDto(HttpEntityManager manager,
			MonitoringMessage.GetPersistentSubscriptionStatsCompleted message) {
			if (message == null) yield break;
			if (message.SubscriptionStats == null) yield break;

			foreach (var stat in message.SubscriptionStats) {
				string escapedStreamId = Uri.EscapeDataString(stat.EventStreamId);
				string escapedGroupName = Uri.EscapeDataString(stat.GroupName);
				var info = new SubscriptionSummary {
					Links = new List<RelLink>() {
						new RelLink(
							MakeUrl(manager,
								string.Format("/subscriptions/{0}/{1}/info", escapedStreamId, escapedGroupName)),
							"detail"),
					},
					EventStreamId = stat.EventStreamId,
					GroupName = stat.GroupName,
					Status = stat.Status,
					AverageItemsPerSecond = stat.AveragePerSecond,
					TotalItemsProcessed = stat.TotalItems,
					LastKnownEventNumber = stat.LastKnownMessage,
					LastProcessedEventNumber = stat.LastProcessedEventNumber,
					ParkedMessageUri = MakeUrl(manager,
						string.Format(parkedMessageUriTemplate, escapedStreamId, escapedGroupName)),
					GetMessagesUri = MakeUrl(manager,
						string.Format("/subscriptions/{0}/{1}/{2}", escapedStreamId, escapedGroupName,
							DefaultNumberOfMessagesToGet)),
					TotalInFlightMessages = stat.TotalInFlightMessages,
				};
				if (stat.Connections != null) {
					info.ConnectionCount = stat.Connections.Count;
				}

				yield return info;
			}
		}

		public class SubscriptionConfigData {
			public bool ResolveLinktos { get; set; }
			public long StartFrom { get; set; }
			public int MessageTimeoutMilliseconds { get; set; }
			public bool ExtraStatistics { get; set; }
			public int MaxRetryCount { get; set; }
			public int LiveBufferSize { get; set; }
			public int BufferSize { get; set; }
			public int ReadBatchSize { get; set; }
			public bool PreferRoundRobin { get; set; }
			public int CheckPointAfterMilliseconds { get; set; }
			public int MinCheckPointCount { get; set; }
			public int MaxCheckPointCount { get; set; }
			public int MaxSubscriberCount { get; set; }
			public string NamedConsumerStrategy { get; set; }

			public SubscriptionConfigData() {
				StartFrom = 0;
				MessageTimeoutMilliseconds = 10000;
				MaxRetryCount = 10;
				CheckPointAfterMilliseconds = 1000;
				MinCheckPointCount = 10;
				MaxCheckPointCount = 500;
				MaxSubscriberCount = 10;
				NamedConsumerStrategy = "RoundRobin";

				BufferSize = 500;
				LiveBufferSize = 500;
				ReadBatchSize = 20;
			}
		}

		public class SubscriptionSummary {
			public List<RelLink> Links { get; set; }
			public string EventStreamId { get; set; }
			public string GroupName { get; set; }
			public string ParkedMessageUri { get; set; }
			public string GetMessagesUri { get; set; }
			public string Status { get; set; }
			public decimal AverageItemsPerSecond { get; set; }
			public long TotalItemsProcessed { get; set; }
			public long LastProcessedEventNumber { get; set; }
			public long LastKnownEventNumber { get; set; }
			public int ConnectionCount { get; set; }
			public int TotalInFlightMessages { get; set; }
		}

		public class SubscriptionInfo {
			public List<RelLink> Links { get; set; }
			public SubscriptionConfigData Config { get; set; }
			public string EventStreamId { get; set; }
			public string GroupName { get; set; }
			public string Status { get; set; }
			public decimal AverageItemsPerSecond { get; set; }
			public string ParkedMessageUri { get; set; }
			public string GetMessagesUri { get; set; }
			public long TotalItemsProcessed { get; set; }
			public long CountSinceLastMeasurement { get; set; }
			public long LastProcessedEventNumber { get; set; }
			public long LastKnownEventNumber { get; set; }
			public int ReadBufferCount { get; set; }
			public long LiveBufferCount { get; set; }
			public int RetryBufferCount { get; set; }
			public int TotalInFlightMessages { get; set; }
			public int OutstandingMessagesCount { get; set; }
			public List<ConnectionInfo> Connections { get; set; }
		}

		public class ConnectionInfo {
			public string From { get; set; }
			public string Username { get; set; }
			public decimal AverageItemsPerSecond { get; set; }
			public long TotalItemsProcessed { get; set; }
			public long CountSinceLastMeasurement { get; set; }
			public List<Measurement> ExtraStatistics { get; set; }
			public int AvailableSlots { get; set; }
			public int InFlightMessages { get; set; }
			public string ConnectionName { get; set; }
		}
	}
}
