using System;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http {
	public interface IHttpController {
		void Subscribe(IHttpService service);
	}

	public interface IHttpSender {
		void SubscribeSenders(HttpMessagePipe pipe);
	}

	public interface IHttpForwarder {
		bool ForwardRequest(HttpEntityManager manager);
	}

	public interface IHttpService {
		ServiceAccessibility Accessibility { get; }

		void RegisterCustomAction(ControllerAction action,
			Func<HttpEntityManager, RequestParams> handler);

		void RegisterAction(ControllerAction action, Action<HttpEntityManager> handler);
	}
}
