exports.handler = (event, context, callback) => {
    event.response.autoConfirmUser = true;
    event.response.autoVerifyEmail = true;
    context.done(null, event);
};
