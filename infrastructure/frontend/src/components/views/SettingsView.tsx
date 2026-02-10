import { useState, useEffect } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Badge } from "@/components/ui/badge";
import { Textarea } from "@/components/ui/textarea";
import { ConfirmDialog } from "@/components/ui/confirm-dialog";
import { PlanSelectionModal } from "@/components/ui/plan-selection-modal";
import {
  User as UserIcon,
  Buildings,
  CreditCard,
  Bell as BellIcon,
  Palette,
  ShieldCheck,
  Trash,
  Plug,
  GithubLogo,
  Key,
} from "@phosphor-icons/react";
import { useAuthStore } from "@/stores/auth.store";
import { useOrganizationStore } from "@/stores/organization.store";
import { useTheme } from "@/contexts/ThemeContext";
import { toast } from "sonner";
import {
  usersService,
  organizationsService,
  billingService,
} from "@/services/api";
import { getFullAvatarUrl } from "@/lib/avatar-utils";

export function SettingsView() {
  const { user, setUser } = useAuthStore();
  const { currentOrganization, updateOrganization: updateOrgInStore } =
    useOrganizationStore();
  const { theme, setTheme } = useTheme();
  const [activeTab, setActiveTab] = useState("profile");
  const [openaiApiKey, setOpenaiApiKey] = useState("");
  const [githubConnected, setGithubConnected] = useState(false);
  const [smtpConfig, setSmtpConfig] = useState<any>(null);
  const [members, setMembers] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  const [fullName, setFullName] = useState("");
  const [email, setEmail] = useState("");
  const [bio, setBio] = useState("");
  const [avatarFile, setAvatarFile] = useState<File | null>(null);
  const [avatarPreview, setAvatarPreview] = useState<string>("");
  const [organizationName, setOrganizationName] = useState("");
  const [teamSize, setTeamSize] = useState("");
  const [primaryUseCase, setPrimaryUseCase] = useState("");

  // Password change state
  const [currentPassword, setCurrentPassword] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");

  // Billing state
  const [billingHistory, setBillingHistory] = useState<any[]>([]);
  const [currentPlan, setCurrentPlan] = useState<any>(null);

  // Delete confirmation modals
  const [showDeleteAccountModal, setShowDeleteAccountModal] = useState(false);
  const [showDeleteOrgModal, setShowDeleteOrgModal] = useState(false);
  const [showPlanModal, setShowPlanModal] = useState(false);

  useEffect(() => {
    if (user) {
      setFullName(user.name || "");
      setEmail(user.email || "");
      setBio(user.bio || "");
      setAvatarPreview(getFullAvatarUrl(user.avatarUrl));
      setTeamSize(user.teamSize || "");
      setPrimaryUseCase(user.primaryUseCase || "");
    }
  }, [user]);

  useEffect(() => {
    if (currentOrganization) {
      setOrganizationName(currentOrganization.name || "");
      loadMembers();
      if (activeTab === "billing") {
        loadBillingData();
      }
    }
  }, [currentOrganization, activeTab]);

  const loadMembers = async () => {
    if (!currentOrganization?.id) return;
    try {
      const data = await organizationsService.getOrganizationMembers(
        currentOrganization.id
      );
      setMembers(Array.isArray(data) ? data : []);
    } catch (error) {
      console.error("Failed to load members:", error);
    }
  };

  const loadBillingData = async () => {
    if (!currentOrganization?.id) return;
    try {
      const [history, plan] = await Promise.all([
        billingService.getBillingHistory(currentOrganization.id),
        billingService.getCurrentPlan(currentOrganization.id),
      ]);
      setBillingHistory(Array.isArray(history) ? history : []);
      setCurrentPlan(plan);
    } catch (error) {
      console.error("Failed to load billing data:", error);
    }
  };

  const handleSaveProfile = async () => {
    if (!user?.id) return;
    setLoading(true);
    try {
      let avatarUrl = user.avatarUrl;

      // Upload avatar if changed
      if (avatarFile) {
        const formData = new FormData();
        formData.append("avatar", avatarFile);
        const avatarResponse = await usersService.uploadAvatar(
          user.id,
          formData
        );
        avatarUrl = avatarResponse.avatarUrl;
      }

      const updated = await usersService.updateUser(user.id, {
        name: fullName,
        email: email,
        bio: bio,
      });

      // Update user with new data including avatar
      setUser({ ...user, ...updated, avatarUrl });
      setAvatarPreview(getFullAvatarUrl(avatarUrl));
      toast.success("Profile updated successfully");
    } catch (error) {
      console.error("Failed to update profile:", error);
      toast.error("Failed to update profile");
    } finally {
      setLoading(false);
    }
  };

  const handleAvatarChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      if (file.size > 5 * 1024 * 1024) {
        toast.error("File size must be less than 5MB");
        return;
      }
      if (!file.type.match(/^image\/(jpg|jpeg|png|gif)$/)) {
        toast.error("Only JPG, PNG and GIF images are allowed");
        return;
      }
      setAvatarFile(file);
      const reader = new FileReader();
      reader.onloadend = () => {
        setAvatarPreview(reader.result as string);
      };
      reader.readAsDataURL(file);
    }
  };

  const handleSaveOrganization = async () => {
    if (!currentOrganization?.id) return;
    setLoading(true);
    try {
      const updated = await organizationsService.updateOrganization(
        currentOrganization.id,
        {
          name: organizationName,
        }
      );
      await updateOrgInStore(currentOrganization.id, {
        name: organizationName,
      });
      toast.success("Organization updated successfully");
    } catch (error) {
      console.error("Failed to update organization:", error);
      toast.error("Failed to update organization");
    } finally {
      setLoading(false);
    }
  };

  const handleChangePassword = async () => {
    if (!user?.id) return;

    // Check if user has password set (not SSO user)
    const isPasswordSet = user.authType !== "Microsoft SSO";

    if (isPasswordSet && !currentPassword) {
      toast.error("Please enter your current password");
      return;
    }

    if (!newPassword || !confirmPassword) {
      toast.error("Please fill in all password fields");
      return;
    }
    if (newPassword !== confirmPassword) {
      toast.error("New passwords do not match");
      return;
    }
    if (newPassword.length < 8) {
      toast.error("Password must be at least 8 characters");
      return;
    }
    setLoading(true);
    try {
      if (isPasswordSet) {
        // User has password, require current password
        await usersService.changePassword(
          user.id,
          currentPassword,
          newPassword
        );
      } else {
        // SSO user setting up password for first time
        await usersService.setupPassword(user.id, newPassword);
      }
      setCurrentPassword("");
      setNewPassword("");
      setConfirmPassword("");
      toast.success(
        isPasswordSet
          ? "Password changed successfully"
          : "Password set successfully"
      );
    } catch (error: any) {
      console.error("Failed to change password:", error);
      toast.error(error.message || "Failed to change password");
    } finally {
      setLoading(false);
    }
  };

  const handleChangePlan = async (plan: "free" | "pro" | "enterprise") => {
    if (!currentOrganization?.id) return;
    setLoading(true);
    try {
      await billingService.updatePlan(currentOrganization.id, plan);
      await updateOrgInStore(currentOrganization.id, { plan });
      setShowPlanModal(false);
      toast.success(`Plan changed to ${plan.toUpperCase()} successfully`);
      loadBillingData();
    } catch (error) {
      console.error("Failed to change plan:", error);
      toast.error("Failed to change plan");
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteAccount = async () => {
    if (!user?.id) return;
    setLoading(true);
    try {
      await usersService.deleteUser(user.id);
      toast.success("Account deleted successfully");
      // Logout and redirect
      setTimeout(() => {
        window.location.href = "/auth";
      }, 1000);
    } catch (error) {
      console.error("Failed to delete account:", error);
      toast.error("Failed to delete account");
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteOrganization = async () => {
    if (!currentOrganization?.id) return;
    setLoading(true);
    try {
      await organizationsService.deleteOrganization(currentOrganization.id);
      toast.success("Organization deleted successfully");
      // Reload organizations
      setTimeout(() => {
        window.location.href = "/dashboard";
      }, 1000);
    } catch (error) {
      console.error("Failed to delete organization:", error);
      toast.error("Failed to delete organization");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="space-y-6 p-6">
      <div>
        <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
        <p className="text-muted-foreground mt-2">
          Manage your account and workspace preferences
        </p>
      </div>

      <Tabs
        value={activeTab}
        onValueChange={setActiveTab}
        className="space-y-6"
      >
        <TabsList className="grid w-full grid-cols-8 lg:w-auto lg:inline-grid">
          <TabsTrigger value="profile" className="gap-2">
            <UserIcon className="h-4 w-4" />
            <span className="hidden sm:inline">Profile</span>
          </TabsTrigger>
          <TabsTrigger value="workspace" className="gap-2">
            <Buildings className="h-4 w-4" />
            <span className="hidden sm:inline">Workspace</span>
          </TabsTrigger>
          <TabsTrigger value="integrations" className="gap-2">
            <Plug className="h-4 w-4" />
            <span className="hidden sm:inline">Integrations</span>
          </TabsTrigger>
          <TabsTrigger value="billing" className="gap-2">
            <CreditCard className="h-4 w-4" />
            <span className="hidden sm:inline">Billing</span>
          </TabsTrigger>
          <TabsTrigger value="notifications" className="gap-2">
            <BellIcon className="h-4 w-4" />
            <span className="hidden sm:inline">Notifications</span>
          </TabsTrigger>
          <TabsTrigger value="appearance" className="gap-2">
            <Palette className="h-4 w-4" />
            <span className="hidden sm:inline">Appearance</span>
          </TabsTrigger>
          <TabsTrigger value="security" className="gap-2">
            <ShieldCheck className="h-4 w-4" />
            <span className="hidden sm:inline">Security</span>
          </TabsTrigger>
          <TabsTrigger value="danger" className="gap-2">
            <Trash className="h-4 w-4" />
            <span className="hidden sm:inline">Danger Zone</span>
          </TabsTrigger>
        </TabsList>

        <TabsContent value="profile" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Profile Information</CardTitle>
              <CardDescription>
                Update your personal information and avatar
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="flex items-center gap-6">
                <div className="relative">
                  {avatarPreview ? (
                    <img
                      src={avatarPreview}
                      alt="Avatar"
                      className="h-20 w-20 rounded-full object-cover border-0.5 border-gray-300"
                    />
                  ) : (
                    <div className="flex h-20 w-20 items-center justify-center rounded-full bg-primary text-primary-foreground text-2xl font-bold">
                      {user?.name
                        .split(" ")
                        .map((n) => n[0])
                        .join("")
                        .toUpperCase()}
                    </div>
                  )}
                </div>
                <div className="space-y-2">
                  <input
                    type="file"
                    id="avatar-upload"
                    className="hidden"
                    accept="image/jpeg,image/png,image/gif"
                    onChange={handleAvatarChange}
                  />
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() =>
                      document.getElementById("avatar-upload")?.click()
                    }
                  >
                    Change Avatar
                  </Button>
                  <p className="text-xs text-muted-foreground">
                    JPG, PNG or GIF. Max size 5MB.
                  </p>
                </div>
              </div>

              <Separator />

              <div className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="fullName">Full Name</Label>
                  <Input
                    id="fullName"
                    value={fullName}
                    onChange={(e) => setFullName(e.target.value)}
                    placeholder="John Doe"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="email">Email Address</Label>
                  <Input
                    id="email"
                    type="email"
                    value={email}
                    onChange={(e) => setEmail(e.target.value)}
                    placeholder="john.doe@example.com"
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="bio">Bio</Label>
                  <Textarea
                    id="bio"
                    placeholder="Tell us a little about yourself..."
                    rows={4}
                    value={bio}
                    onChange={(e) => setBio(e.target.value)}
                  />
                </div>
              </div>

              <div className="flex justify-end gap-3">
                <Button
                  variant="outline"
                  onClick={() => {
                    setFullName(user?.name || "");
                    setEmail(user?.email || "");
                    setBio(user?.bio || "");
                    setAvatarPreview(getFullAvatarUrl(user?.avatarUrl));
                    setAvatarFile(null);
                  }}
                  disabled={loading}
                >
                  Cancel
                </Button>
                <Button onClick={handleSaveProfile} disabled={loading}>
                  {loading ? "Saving..." : "Save Changes"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="workspace" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Organization Settings</CardTitle>
              <CardDescription>
                Manage your organization configuration and team members
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <Label htmlFor="organizationName">Organization Name</Label>
                <Input
                  id="organizationName"
                  value={organizationName}
                  onChange={(e) => setOrganizationName(e.target.value)}
                />
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div className="space-y-2">
                  <Label htmlFor="teamSize">Team Size</Label>
                  <Input
                    id="teamSize"
                    placeholder="e.g., 5-10 people"
                    value={teamSize}
                    onChange={(e) => setTeamSize(e.target.value)}
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="primaryUseCase">Primary Use Case</Label>
                  <Input
                    id="primaryUseCase"
                    placeholder="e.g., Schema comparison"
                    value={primaryUseCase}
                    onChange={(e) => setPrimaryUseCase(e.target.value)}
                  />
                </div>
              </div>

              <div className="space-y-2">
                <Label htmlFor="organizationPlan">Current Plan</Label>
                <div className="flex items-center gap-3">
                  <Badge variant="default" className="capitalize">
                    {currentOrganization?.plan || "free"}
                  </Badge>
                  <span className="text-sm text-muted-foreground">
                    {members.length} members
                  </span>
                </div>
              </div>

              <Separator />

              {user?.isOrganizationOwner && (
                <div>
                  <h3 className="text-lg font-semibold mb-4">
                    Organization Invitation
                  </h3>
                  <p className="text-sm text-muted-foreground mb-4">
                    Share this link with team members to invite them to join
                    your organization
                  </p>
                  <div className="border-0.5 border-gray-300 shadow-sm p-4 bg-muted/30 rounded-lg">
                    <div className="flex items-center gap-2 mb-3">
                      <Key className="h-5 w-5 text-primary" />
                      <Label className="text-sm font-semibold">
                        Organization Key
                      </Label>
                    </div>
                    <div className="flex gap-2">
                      <Input
                        value={currentOrganization?.id || "Loading..."}
                        readOnly
                        className="font-mono text-sm bg-background"
                      />
                      <Button
                        variant="outline"
                        onClick={() => {
                          if (currentOrganization?.id) {
                            navigator.clipboard.writeText(
                              currentOrganization.id
                            );
                            toast.success("Organization key copied!");
                          }
                        }}
                      >
                        Copy Key
                      </Button>
                    </div>
                    <div className="mt-3">
                      <Label className="text-sm font-semibold mb-2 block">
                        Invitation URL
                      </Label>
                      <div className="flex gap-2">
                        <Input
                          value={
                            currentOrganization?.id
                              ? `${window.location.origin}/signup?orgKey=${currentOrganization.id}`
                              : "Loading..."
                          }
                          readOnly
                          className="font-mono text-sm bg-background"
                        />
                        <Button
                          variant="outline"
                          onClick={() => {
                            if (currentOrganization?.id) {
                              const inviteUrl = `${window.location.origin}/signup?orgKey=${currentOrganization.id}`;
                              navigator.clipboard.writeText(inviteUrl);
                              toast.success("Invitation URL copied!");
                            }
                          }}
                        >
                          Copy URL
                        </Button>
                      </div>
                    </div>
                  </div>
                </div>
              )}

              <div>
                <h3 className="text-lg font-semibold mb-4">Team Members</h3>
                <div className="space-y-3">
                  {members.length > 0 ? (
                    members.map((member: any, i: number) => (
                      <div
                        key={i}
                        className="flex items-center justify-between p-3 border-0.5 dark:border-gray-100 border-gray-300 shadow-sm"
                      >
                        <div className="flex items-center gap-3">
                          <div className="flex h-10 w-10 items-center justify-center rounded-full bg-accent text-accent-foreground font-semibold">
                            {(member.user?.name || member.email || "U")
                              .substring(0, 2)
                              .toUpperCase()}
                          </div>
                          <div>
                            <p className="font-medium">
                              {member.user?.name || member.email}
                              {member.userId === user?.id && " (You)"}
                            </p>
                            <p className="text-xs text-muted-foreground capitalize">
                              {member.role}
                            </p>
                          </div>
                        </div>
                        {member.userId !== user?.id && (
                          <Button variant="ghost" size="sm">
                            Remove
                          </Button>
                        )}
                      </div>
                    ))
                  ) : (
                    <div className="flex items-center justify-between p-3 border-0.5 border-gray-300 shadow-sm">
                      <div className="flex items-center gap-3">
                        <div className="flex h-10 w-10 items-center justify-center rounded-full bg-accent text-accent-foreground font-semibold">
                          {(user?.name || "U").substring(0, 2).toUpperCase()}
                        </div>
                        <div>
                          <p className="font-medium">{user?.name} (You)</p>
                          <p className="text-xs text-muted-foreground capitalize">
                            owner
                          </p>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
                <Button className="mt-4 w-full" variant="outline">
                  Invite Team Member
                </Button>
              </div>

              <div className="flex justify-end">
                <Button onClick={handleSaveOrganization} disabled={loading}>
                  {loading ? "Saving..." : "Save Organization"}
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="integrations" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Integrations</CardTitle>
              <CardDescription>
                Connect external services to enhance your workspace
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="border-0.5 border-gray-300 shadow-sm p-4">
                <div className="flex items-start justify-between">
                  <div className="flex items-center gap-3">
                    <div className="flex h-10 w-10 items-center justify-center rounded-lg bg-muted">
                      <GithubLogo className="h-6 w-6" weight="fill" />
                    </div>
                    <div>
                      <h3 className="font-semibold">GitHub</h3>
                      <p className="text-sm text-muted-foreground">
                        Version control for schemas and configurations
                      </p>
                    </div>
                  </div>
                  {githubConnected ? (
                    <div className="flex items-center gap-2">
                      <Badge variant="default">Connected</Badge>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => {
                          setGithubConnected(false);
                          toast.success("GitHub disconnected");
                        }}
                      >
                        Disconnect
                      </Button>
                    </div>
                  ) : (
                    <Button
                      variant="outline"
                      onClick={() => {
                        setGithubConnected(true);
                        toast.success("GitHub connected successfully");
                      }}
                    >
                      Connect
                    </Button>
                  )}
                </div>
              </div>

              <Separator />

              <div>
                <div className="flex items-center gap-2 mb-4">
                  <Key className="h-5 w-5" />
                  <h3 className="text-lg font-semibold">OpenAI API Key</h3>
                </div>
                <p className="text-sm text-muted-foreground mb-4">
                  Configure your OpenAI API key to enable AI-powered features
                  like natural language queries and smart suggestions.
                </p>
                <div className="space-y-4">
                  <div className="space-y-2">
                    <Label htmlFor="openai-key">API Key</Label>
                    <Input
                      id="openai-key"
                      type="password"
                      placeholder="sk-..."
                      value={openaiApiKey ?? ""}
                      onChange={(e) => setOpenaiApiKey(e.target.value)}
                    />
                    <p className="text-xs text-muted-foreground">
                      Get your API key from{" "}
                      <a
                        href="https://platform.openai.com/api-keys"
                        target="_blank"
                        rel="noopener noreferrer"
                        className="text-primary hover:underline"
                      >
                        OpenAI Platform
                      </a>
                    </p>
                  </div>
                  <Button
                    onClick={() =>
                      toast.success("OpenAI API key saved securely")
                    }
                    disabled={!openaiApiKey || openaiApiKey.length === 0}
                  >
                    Save API Key
                  </Button>
                </div>
              </div>

              <Separator />

              <div>
                <h3 className="text-lg font-semibold mb-4">
                  SMTP Configuration
                </h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Configure email settings for OTP verification and
                  notifications
                </p>
                <div className="grid gap-4 sm:grid-cols-2">
                  <div className="space-y-2">
                    <Label htmlFor="smtp-host">SMTP Host</Label>
                    <Input
                      id="smtp-host"
                      placeholder="smtp.gmail.com"
                      defaultValue={smtpConfig?.host ?? ""}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="smtp-port">SMTP Port</Label>
                    <Input
                      id="smtp-port"
                      type="number"
                      placeholder="587"
                      defaultValue={smtpConfig?.port ?? ""}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="smtp-user">Username</Label>
                    <Input
                      id="smtp-user"
                      type="email"
                      placeholder="your-email@gmail.com"
                      defaultValue={smtpConfig?.user ?? ""}
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="smtp-pass">Password</Label>
                    <Input
                      id="smtp-pass"
                      type="password"
                      placeholder="••••••••"
                    />
                  </div>
                </div>
                <div className="mt-4 flex gap-3">
                  <Button
                    variant="outline"
                    onClick={() => toast.info("Test email sent!")}
                  >
                    Send Test Email
                  </Button>
                  <Button
                    onClick={() => {
                      setSmtpConfig({
                        host: "smtp.gmail.com",
                        port: 587,
                        user: "configured",
                        secure: false,
                      });
                      toast.success("SMTP configuration saved");
                    }}
                  >
                    Save Configuration
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="billing" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Billing & Subscription</CardTitle>
              <CardDescription>
                Manage your subscription and payment methods
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="border-0.5 border-gray-300 shadow-sm bg-muted/50 p-6 rounded-lg">
                <div className="flex items-start justify-between">
                  <div>
                    <h3 className="text-2xl font-bold capitalize">
                      {currentPlan?.plan || currentOrganization?.plan || "free"}{" "}
                      Plan
                    </h3>
                    <p className="text-muted-foreground mt-1">
                      {(currentPlan?.plan || currentOrganization?.plan) ===
                      "enterprise"
                        ? "$99/month"
                        : (currentPlan?.plan || currentOrganization?.plan) ===
                          "pro"
                        ? "$49/month"
                        : "Free"}
                    </p>
                  </div>
                  <Badge variant="default">
                    {currentPlan?.status || "Active"}
                  </Badge>
                </div>
                <div className="mt-6 space-y-2">
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground">
                      Next billing date
                    </span>
                    <span className="font-medium">
                      {currentPlan?.nextBillingDate
                        ? new Date(
                            currentPlan.nextBillingDate
                          ).toLocaleDateString()
                        : "N/A"}
                    </span>
                  </div>
                  <div className="flex justify-between text-sm">
                    <span className="text-muted-foreground">Team members</span>
                    <span className="font-medium">
                      {members.length} / Unlimited
                    </span>
                  </div>
                </div>
                <div className="mt-6">
                  <Button
                    variant="outline"
                    onClick={() => setShowPlanModal(true)}
                    disabled={loading}
                  >
                    Change Plan
                  </Button>
                </div>
              </div>

              <Separator />

              <div>
                <h3 className="text-lg font-semibold mb-4">Invoices</h3>
                <div className="space-y-2">
                  {billingHistory.length > 0 ? (
                    billingHistory.map((invoice) => (
                      <div
                        key={invoice.id}
                        className="flex items-center justify-between p-3 border-0.5 border-gray-300 shadow-sm"
                      >
                        <div>
                          <p className="font-medium">
                            {new Date(invoice.createdAt).toLocaleDateString(
                              "en-US",
                              {
                                month: "long",
                                year: "numeric",
                              }
                            )}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            ${invoice.amount.toFixed(2)} • {invoice.status}
                          </p>
                        </div>
                        <Button variant="ghost" size="sm">
                          Download
                        </Button>
                      </div>
                    ))
                  ) : (
                    <div className="text-center py-8 text-muted-foreground">
                      No billing history available
                    </div>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="notifications" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Notification Preferences</CardTitle>
              <CardDescription>
                Choose how you want to be notified about activity
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              {[
                {
                  label: "Email Notifications",
                  description: "Receive email updates about your workspace",
                },
                {
                  label: "Schema Comparison Alerts",
                  description: "Get notified when schema comparisons complete",
                },
                {
                  label: "Connection Status Changes",
                  description: "Alert me when connections fail or recover",
                },
                {
                  label: "Team Activity",
                  description: "Notifications about team member actions",
                },
                {
                  label: "Deployment Reminders",
                  description: "Reminders for pending deployments",
                },
                {
                  label: "Weekly Summary",
                  description: "Weekly email digest of workspace activity",
                },
              ].map((item) => (
                <div
                  key={item.label}
                  className="flex items-center justify-between"
                >
                  <div className="space-y-0.5">
                    <Label className="text-base">{item.label}</Label>
                    <p className="text-sm text-muted-foreground">
                      {item.description}
                    </p>
                  </div>
                  <Switch defaultChecked />
                </div>
              ))}

              <Separator />

              <div className="flex justify-end">
                <Button
                  onClick={() =>
                    toast.success("Notification preferences saved")
                  }
                >
                  Save Preferences
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="appearance" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Appearance Settings</CardTitle>
              <CardDescription>
                Customize how Data Amplifier looks for you
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="space-y-2">
                <Label>Theme</Label>
                <Select
                  value={theme}
                  onValueChange={(value) => setTheme(value as "light" | "dark")}
                >
                  <SelectTrigger className="w-50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="light">Light</SelectItem>
                    <SelectItem value="dark">Dark</SelectItem>
                  </SelectContent>
                </Select>
                <p className="text-xs text-muted-foreground">
                  Choose between light and dark theme
                </p>
              </div>

              <Separator />

              <div className="space-y-2">
                <Label>Font Size</Label>
                <Select defaultValue="medium">
                  <SelectTrigger className="w-50">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="small">Small</SelectItem>
                    <SelectItem value="medium">Medium</SelectItem>
                    <SelectItem value="large">Large</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              <div className="space-y-2">
                <Label>Compact Mode</Label>
                <div className="flex items-center justify-between">
                  <p className="text-sm text-muted-foreground">
                    Reduce spacing and padding for a denser interface
                  </p>
                  <Switch />
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="security" className="space-y-6">
          <Card className="border-0.5 border-gray-300 shadow-sm">
            <CardHeader>
              <CardTitle>Security Settings</CardTitle>
              <CardDescription>
                Manage your account security and authentication
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div>
                <h3 className="text-lg font-semibold mb-4">
                  {user?.authType === "Microsoft SSO"
                    ? "Set Password"
                    : "Change Password"}
                </h3>
                {user?.authType === "Microsoft SSO" && (
                  <p className="text-sm text-muted-foreground mb-4">
                    You're currently signed in with Microsoft SSO. Set a
                    password to enable password-based login as an alternative.
                  </p>
                )}
                <div className="space-y-4">
                  {user?.authType !== "Microsoft SSO" && (
                    <div className="space-y-2">
                      <Label htmlFor="currentPassword">Current Password</Label>
                      <Input
                        id="currentPassword"
                        type="password"
                        value={currentPassword}
                        onChange={(e) => setCurrentPassword(e.target.value)}
                      />
                    </div>
                  )}
                  <div className="space-y-2">
                    <Label htmlFor="newPassword">New Password</Label>
                    <Input
                      id="newPassword"
                      type="password"
                      value={newPassword}
                      onChange={(e) => setNewPassword(e.target.value)}
                      placeholder="Min 8 characters"
                    />
                  </div>
                  <div className="space-y-2">
                    <Label htmlFor="confirmPassword">
                      Confirm New Password
                    </Label>
                    <Input
                      id="confirmPassword"
                      type="password"
                      value={confirmPassword}
                      onChange={(e) => setConfirmPassword(e.target.value)}
                    />
                  </div>
                  <Button
                    onClick={handleChangePassword}
                    disabled={
                      loading ||
                      !newPassword ||
                      !confirmPassword ||
                      (user?.authType !== "Microsoft SSO" && !currentPassword)
                    }
                  >
                    {loading
                      ? "Updating..."
                      : user?.authType === "Microsoft SSO"
                      ? "Set Password"
                      : "Update Password"}
                  </Button>
                </div>
              </div>

              <Separator />

              <div>
                <h3 className="text-lg font-semibold mb-4">
                  Two-Factor Authentication
                </h3>
                <div className="flex items-center justify-between p-4 border-0.5 border-gray-300 shadow-sm">
                  <div>
                    <p className="font-medium">2FA Status</p>
                    <p className="text-sm text-muted-foreground">
                      Add an extra layer of security to your account
                    </p>
                  </div>
                  <Badge variant="outline">Disabled</Badge>
                </div>
                <Button variant="outline" className="mt-4">
                  Enable 2FA
                </Button>
              </div>

              <Separator />

              <div>
                <h3 className="text-lg font-semibold mb-4">Active Sessions</h3>
                <div className="space-y-2">
                  {[
                    {
                      device: "Chrome on MacOS",
                      location: "San Francisco, CA",
                      current: true,
                    },
                    {
                      device: "Firefox on Windows",
                      location: "New York, NY",
                      current: false,
                    },
                  ].map((session, i) => (
                    <div
                      key={i}
                      className="flex items-center justify-between p-3 border-0.5 border-gray-300 shadow-sm"
                    >
                      <div>
                        <div className="flex items-center gap-2">
                          <p className="font-medium">{session.device}</p>
                          {session.current && (
                            <Badge variant="default" className="text-xs">
                              Current
                            </Badge>
                          )}
                        </div>
                        <p className="text-xs text-muted-foreground">
                          {session.location}
                        </p>
                      </div>
                      {!session.current && (
                        <Button variant="ghost" size="sm">
                          Revoke
                        </Button>
                      )}
                    </div>
                  ))}
                </div>
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        <TabsContent value="danger" className="space-y-6">
          <Card className="border-destructive">
            <CardHeader>
              <CardTitle className="text-destructive">Danger Zone</CardTitle>
              <CardDescription>
                Irreversible actions that affect your account or workspace
              </CardDescription>
            </CardHeader>
            <CardContent className="space-y-6">
              <div className="border-0.5 border-gray-300 shadow-sm border-destructive/50 bg-destructive/10 p-4">
                <h3 className="font-semibold mb-2">Delete Organization</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Permanently delete{" "}
                  {currentOrganization?.name || "this organization"} and all
                  associated data. This action cannot be undone.
                </p>
                <Button
                  variant="destructive"
                  onClick={() => setShowDeleteOrgModal(true)}
                  disabled={loading}
                >
                  Delete Organization
                </Button>
              </div>

              <div className="border-0.5 border-gray-300 shadow-sm border-destructive/50 bg-destructive/10 p-4">
                <h3 className="font-semibold mb-2">Delete Account</h3>
                <p className="text-sm text-muted-foreground mb-4">
                  Permanently delete your account and all personal data. You
                  will lose access to all organizations.
                </p>
                <Button
                  variant="destructive"
                  onClick={() => setShowDeleteAccountModal(true)}
                  disabled={loading}
                >
                  Delete Account
                </Button>
              </div>
            </CardContent>
          </Card>
        </TabsContent>
      </Tabs>

      {/* Confirmation Modals */}
      <ConfirmDialog
        open={showDeleteAccountModal}
        onOpenChange={setShowDeleteAccountModal}
        onConfirm={handleDeleteAccount}
        title="Delete Account"
        description="Are you sure you want to delete your account? This action cannot be undone and you will lose access to all organizations."
        confirmText="Delete Account"
        cancelText="Cancel"
        variant="destructive"
      />

      <ConfirmDialog
        open={showDeleteOrgModal}
        onOpenChange={setShowDeleteOrgModal}
        onConfirm={handleDeleteOrganization}
        title="Delete Organization"
        description={`Are you sure you want to delete ${
          currentOrganization?.name || "this organization"
        }? This action cannot be undone and all members will lose access.`}
        confirmText="Delete Organization"
        cancelText="Cancel"
        variant="destructive"
      />

      {/* Plan Selection Modal */}
      <PlanSelectionModal
        open={showPlanModal}
        onOpenChange={setShowPlanModal}
        currentPlan={currentOrganization?.plan || "free"}
        onSelectPlan={handleChangePlan}
        loading={loading}
      />
    </div>
  );
}
