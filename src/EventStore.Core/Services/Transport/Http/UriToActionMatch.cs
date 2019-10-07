using System;
using EventStore.Transport.Http.EntityManagement;

namespace EventStore.Core.Services.Transport.Http {
	public class UriToActionMatch {
		public readonly ControllerAction ControllerAction;
		public readonly Func<HttpEntityManager, RequestParams> RequestHandler;

		public UriToActionMatch(
			ControllerAction controllerAction,
			Func<HttpEntityManager, RequestParams> requestHandler) {
			ControllerAction = controllerAction;
			RequestHandler = requestHandler;
		}
	}
}
