"""Merge / join action node."""

NODE_TYPE = "action.merge"
LABEL = "Merge / Join"


def run(config, inp, context, logger, creds=None, **kwargs):
    """
    Merge outputs from multiple upstream nodes.
    mode=first : pass through first upstream value (default)
    mode=all   : collect all upstream node outputs into a list
    mode=dict  : merge all upstream dicts into one dict (last wins on collision)
    """
    mode = config.get('mode', 'dict')
    upstream_ids = kwargs.get('upstream_ids', [])

    if mode == 'first' or not upstream_ids:
        return inp

    upstream_outputs = [context.get(uid) for uid in upstream_ids if context.get(uid) is not None]

    if mode == 'all':
        return {'merged': upstream_outputs, 'count': len(upstream_outputs)}

    elif mode == 'dict':
        result = {}
        for out in upstream_outputs:
            if isinstance(out, dict):
                result.update(out)
        return result

    else:
        return inp

