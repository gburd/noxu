# Your dedicated EC2 box (lock)

i4i.8xlarge, 32 vCPU, NVMe XFS at /data.
**You are the ONLY agent on this box** — your measurements are clean. Say so in
your report, and do not co-schedule anything else heavy on it.

    export AWS_PROFILE=bene
    SSH="ssh -i /tmp/noxu-lock-1789043624.pem -o IdentitiesOnly=yes -o IdentityAgent=none -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null ec2-user@18.223.117.250"

Upload your worktree (exclude target/ and .git, they are huge):

    tar czf /tmp/mine.tgz --exclude=target --exclude=.git -C /tmp/w-resv2 .
    scp -i /tmp/noxu-lock-1789043624.pem -o IdentitiesOnly=yes -o IdentityAgent=none -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -q /tmp/mine.tgz ec2-user@18.223.117.250:/data/
    $SSH 'mkdir -p /data/work && tar xzf /data/mine.tgz -C /data/work'

RULES: never bench on tmpfs (use /data); measure the STEADY phase; interleave
baseline-vs-change and repeat 3x; bound every command with `timeout`;
cargo-nextest is installed. DO NOT terminate the instance.
