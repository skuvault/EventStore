using System;
using EventStore.Core.Bus;
using EventStore.Core.Messages;
using EventStore.Common.Log;
using EventStore.Core.Messaging;
using EventStore.Transport.Http;
using EventStore.Transport.Http.Codecs;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http.Controllers {
	public class UsersController : CommunicationController {
		private readonly IHttpForwarder _httpForwarder;
		private readonly IPublisher _networkSendQueue;
		private static readonly ICodec[] DefaultCodecs = new ICodec[] {Codec.Json, Codec.Xml};
		private static readonly ILogger Log = LogManager.GetLoggerFor<UsersController>();

		public UsersController(IHttpForwarder httpForwarder, IPublisher publisher, IPublisher networkSendQueue)
			: base(publisher) {
			_httpForwarder = httpForwarder;
			_networkSendQueue = networkSendQueue;
		}

		protected override void SubscribeCore(IHttpService service) {
			RegisterUrlBased(service, "/users", HttpMethod.Get, AuthorizationLevel.Admin, GetUsers);
			RegisterUrlBased(service, "/users/", HttpMethod.Get, AuthorizationLevel.Admin, GetUsers);
			RegisterUrlBased(service, "/users/{login}", HttpMethod.Get, AuthorizationLevel.Admin, GetUser);
			RegisterUrlBased(service, "/users/$current", HttpMethod.Get, AuthorizationLevel.User, GetCurrentUser);
			Register(service, "/users", HttpMethod.Post, PostUser, DefaultCodecs, DefaultCodecs, AuthorizationLevel.Admin);
			Register(service, "/users/", HttpMethod.Post, PostUser, DefaultCodecs, DefaultCodecs, AuthorizationLevel.Admin);
			Register(service, "/users/{login}", HttpMethod.Put, PutUser, DefaultCodecs, DefaultCodecs, AuthorizationLevel.Admin);
			RegisterUrlBased(service, "/users/{login}", HttpMethod.Delete, AuthorizationLevel.Admin, DeleteUser);
			RegisterUrlBased(service, "/users/{login}/command/enable", HttpMethod.Post, AuthorizationLevel.Admin, PostCommandEnable);
			RegisterUrlBased(service, "/users/{login}/command/disable", HttpMethod.Post, AuthorizationLevel.Admin, PostCommandDisable);
			Register(service, "/users/{login}/command/reset-password", HttpMethod.Post, PostCommandResetPassword,
				DefaultCodecs, DefaultCodecs, AuthorizationLevel.Admin);
			Register(service, "/users/{login}/command/change-password", HttpMethod.Post, PostCommandChangePassword,
				DefaultCodecs, DefaultCodecs, AuthorizationLevel.User);
		}

		private void GetUsers(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

			var envelope = CreateSendToHttpWithConversionEnvelope(http,
				(UserManagementMessage.AllUserDetailsResult msg) =>
					new UserManagementMessage.AllUserDetailsResultHttpFormatted(msg, s => MakeUrl(http, s)));

			var message = new UserManagementMessage.GetAll(envelope, http.User);
			Publish(message);
		}

		private void GetUser(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;

			var envelope = CreateSendToHttpWithConversionEnvelope(http,
				(UserManagementMessage.UserDetailsResult msg) =>
					new UserManagementMessage.UserDetailsResultHttpFormatted(msg, s => MakeUrl(http, s)));

		}

		private void GetCurrentUser(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
			var envelope = CreateReplyEnvelope<UserManagementMessage.UserDetailsResult>(http);
			if (http.User == null) {
				envelope.ReplyWith(
					new UserManagementMessage.UserDetailsResult(UserManagementMessage.Error.Unauthorized));
				return;
			}

			var message = new UserManagementMessage.Get(envelope, http.User, http.User.Identity.Name);
			Publish(message);
		}

		private void PostUser(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
			var envelope = CreateReplyEnvelope<UserManagementMessage.UpdateResult>(
				http, configurator: (codec, result) => {
					var configuration = AutoConfigurator(codec, result);
					return configuration.Code == HttpStatusCode.OK
						? configuration.SetCreated(
							MakeUrl(http, "/users/" + Uri.EscapeDataString(result.LoginName)))
						: configuration;
				});
			http.ReadTextRequestAsync(
				(o, s) => {
					var data = http.RequestCodec.From<PostUserData>(s);
					var message = new UserManagementMessage.Create(
						envelope, http.User, data.LoginName, data.FullName, data.Groups, data.Password);
					Publish(message);
				}, x => Log.DebugException(x, "Reply Text Content Failed."));
		}

		private void PutUser(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
			var envelope = CreateReplyEnvelope<UserManagementMessage.UpdateResult>(http);

		}

		private void DeleteUser(HttpEntityManager http) {
		}

		private void PostCommandEnable(HttpEntityManager http) {
		}

		private void PostCommandDisable(HttpEntityManager http) {
		}

		private void PostCommandResetPassword(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
		}

		private void PostCommandChangePassword(HttpEntityManager http) {
			if (_httpForwarder.ForwardRequest(http))
				return;
			var envelope = CreateReplyEnvelope<UserManagementMessage.UpdateResult>(http);
		}

		private SendToHttpEnvelope<T> CreateReplyEnvelope<T>(
			HttpEntityManager http, Func<ICodec, T, string> formatter = null,
			Func<ICodec, T, ResponseConfiguration> configurator = null)
			where T : UserManagementMessage.ResponseMessage {
			return new SendToHttpEnvelope<T>(
				_networkSendQueue, http, formatter ?? AutoFormatter, configurator ?? AutoConfigurator, null);
		}

		private SendToHttpWithConversionEnvelope<T, R> CreateSendToHttpWithConversionEnvelope<T, R>(
			HttpEntityManager http, Func<T, R> formatter)
			where T : UserManagementMessage.ResponseMessage
			where R : UserManagementMessage.ResponseMessage {
			return new SendToHttpWithConversionEnvelope<T, R>(_networkSendQueue,
				http,
				(codec, msg) => codec.To(msg),
				(codec, transformed) => transformed.Success
					? new ResponseConfiguration(HttpStatusCode.OK, codec.ContentType, codec.Encoding)
					: new ResponseConfiguration(
						ErrorToHttpStatusCode(transformed.Error), codec.ContentType, codec.Encoding),
				formatter);
		}

		private ResponseConfiguration AutoConfigurator<T>(ICodec codec, T result)
			where T : UserManagementMessage.ResponseMessage {
			return result.Success
				? new ResponseConfiguration(HttpStatusCode.OK, codec.ContentType, codec.Encoding)
				: new ResponseConfiguration(
					ErrorToHttpStatusCode(result.Error), codec.ContentType, codec.Encoding);
		}

		private string AutoFormatter<T>(ICodec codec, T result) {
			return codec.To(result);
		}

		private int ErrorToHttpStatusCode(UserManagementMessage.Error error) {
			switch (error) {
				case UserManagementMessage.Error.Success:
					return HttpStatusCode.OK;
				case UserManagementMessage.Error.Conflict:
					return HttpStatusCode.Conflict;
				case UserManagementMessage.Error.NotFound:
					return HttpStatusCode.NotFound;
				case UserManagementMessage.Error.Error:
					return HttpStatusCode.InternalServerError;
				case UserManagementMessage.Error.TryAgain:
					return HttpStatusCode.RequestTimeout;
				case UserManagementMessage.Error.Unauthorized:
					return HttpStatusCode.Unauthorized;
				default:
					return HttpStatusCode.InternalServerError;
			}
		}

		private class PostUserData {
			public string LoginName { get; set; }
			public string FullName { get; set; }
			public string[] Groups { get; set; }
			public string Password { get; set; }
		}

		private class PutUserData {
			public string FullName { get; set; }
			public string[] Groups { get; set; }
		}

		private class ResetPasswordData {
			public string NewPassword { get; set; }
		}

		private class ChangePasswordData {
			public string CurrentPassword { get; set; }
			public string NewPassword { get; set; }
		}
	}
}
