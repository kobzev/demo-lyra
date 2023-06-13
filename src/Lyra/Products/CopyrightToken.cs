namespace Lyra.Products
{
    using System;
    using System.Collections.Generic;
    using Newtonsoft.Json;

    public class CopyrightToken : Product
    {
        private const decimal MinPercentageValue = 10;
        private const decimal MaxPercentageValue = 90;

        public CopyrightToken(
            string productId,
            string externalMusicId,
            string creatorId,
            string icon,
            string color,
            string tenantId,
            SubType subType,
            string ownership,
            decimal amount,
            decimal tradingVolume,
            SongDetails songDetails)
            : base(productId, ProductTypes.CopyrightToken, color)
        {
            if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentNullException(nameof(productId));
            //if (string.IsNullOrWhiteSpace(externalMusicId)) throw new ArgumentNullException(nameof(externalMusicId));
            //if (string.IsNullOrWhiteSpace(creatorId)) throw new ArgumentNullException(nameof(creatorId));
            //if (string.IsNullOrWhiteSpace(icon)) throw new ArgumentNullException(nameof(icon));
            //if (string.IsNullOrWhiteSpace(color)) throw new ArgumentNullException(nameof(color));
            //if (string.IsNullOrWhiteSpace(ownership)) throw new ArgumentNullException(nameof(ownership));
            if (string.IsNullOrWhiteSpace(tenantId)) throw new ArgumentNullException(nameof(tenantId));
            //if (songDetails == null) throw new ArgumentNullException(nameof(songDetails));

            this.ExternalMusicId = externalMusicId;
            this.CreatorId = creatorId;
            this.Icon = icon;
            this.SongDetails = songDetails;
            this.SubType = subType;
            this.Ownership = ownership;
            this.Amount = amount;
            this.TenantId = tenantId;
            this.AlreadyAuctionedAmount = 0;
            this.IsAvailableAtSecondaryMarket = false;
            this.TradingVolume = tradingVolume;
        }

        private CopyrightToken(
            string productId,
            string externalMusicId,
            string creatorId,
            string icon,
            string color,
            string tenantId,
            SubType subType,
            string ownership,
            decimal amount,
            SongDetails songDetails,
            decimal alreadyAuctionedAmount,
            bool isAvailableAtSecondaryMarket,
            bool isMinted,
            decimal tradingVolume)
            : base(productId, ProductTypes.CopyrightToken, color)
        {
            if (string.IsNullOrWhiteSpace(productId)) throw new ArgumentNullException(nameof(productId));
            //if (string.IsNullOrWhiteSpace(externalMusicId)) throw new ArgumentNullException(nameof(externalMusicId));
            //if (string.IsNullOrWhiteSpace(creatorId)) throw new ArgumentNullException(nameof(creatorId));
            //if (string.IsNullOrWhiteSpace(icon)) throw new ArgumentNullException(nameof(icon));
            //if (string.IsNullOrWhiteSpace(color)) throw new ArgumentNullException(nameof(color));
            //if (string.IsNullOrWhiteSpace(ownership)) throw new ArgumentNullException(nameof(ownership));
            if (string.IsNullOrWhiteSpace(tenantId)) throw new ArgumentNullException(nameof(tenantId));
            //if (songDetails == null) throw new ArgumentNullException(nameof(songDetails));

            this.ExternalMusicId = externalMusicId;
            this.CreatorId = creatorId;
            this.Icon = icon;
            this.SongDetails = songDetails;
            this.SubType = subType;
            this.Ownership = ownership;
            this.Amount = amount;
            this.TenantId = tenantId;
            this.AlreadyAuctionedAmount = alreadyAuctionedAmount;
            this.IsAvailableAtSecondaryMarket = isAvailableAtSecondaryMarket;
            this.TradingVolume = tradingVolume;
            if (isMinted) this.SetAsMinted();
        }

        public bool IsAvailableAtSecondaryMarket { get; private set; }
        public string CreatorId { get; }
        public string ExternalMusicId { get; }
        public string Icon { get; }
        public string TenantId { get; }
        public SubType SubType { get; }
        public string Ownership { get; }
        public decimal Amount { get; }
        public decimal AlreadyAuctionedAmount { get; private set; }
        public decimal TradingVolume { get; private set; }
        public SongDetails SongDetails { set; get; }

        public void Auctioned(decimal amount)
        {
            this.AlreadyAuctionedAmount += amount;
            this.IsAvailableAtSecondaryMarket = true;
        }

        public void Traded(decimal amount)
        {
            this.TradingVolume += amount;
        }

        public void SetMiningSettings(decimal miningByStreaming, decimal miningByCuration)
        {
            if (SubType != SubType.Diamond)
            {
                throw new InvalidOperationException("Mining settings available only for diamond tokens");
            }

            if (miningByStreaming < MinPercentageValue || miningByStreaming > MaxPercentageValue)
            {
                throw new ArgumentOutOfRangeException(nameof(miningByStreaming), "Value should be between 10 and 90");
            }

            if (miningByCuration < MinPercentageValue || miningByCuration > MaxPercentageValue)
            {
                throw new ArgumentOutOfRangeException(nameof(miningByCuration), "Value should be between 10 and 90");
            }

            this.SongDetails.MiningByStreaming = miningByStreaming;
            this.SongDetails.MiningByCuration = miningByCuration;
        }

        public static CopyrightToken RestoreFromDatabase(
            string productId,
            string externalMusicId,
            string creatorId,
            string icon,
            string color,
            string tenantId,
            string subType,
            string ownership,
            decimal amount,
            string songDetails,
            decimal alreadyAuctionedAmount,
            bool isAvailableAtSecondaryMarket,
            bool isMinted,
            decimal tradingVolume,
            string instrumentId,
            int ProductStatus)

        {
            Enum.TryParse(subType, out SubType type);

            var token = new CopyrightToken(
                productId,
                externalMusicId,
                creatorId,
                icon,
                color,
                tenantId,
                type,
                ownership,
                amount,
                JsonConvert.DeserializeObject<SongDetails>(songDetails),
                alreadyAuctionedAmount,
                isAvailableAtSecondaryMarket,
                isMinted,
                tradingVolume
                )
            {
                ProductStatus = ProductStatus,
                InstrumentId =instrumentId

            };
            return token;
        }

        public void AddContributors(Contributors contributors)
        {
            if (contributors.Owners != null)
                this.SongDetails.Contributors.Owners.AddRange(contributors.Owners);
            if (contributors.Composers != null)
                this.SongDetails.Contributors.Composers.AddRange(contributors.Composers);
            if (contributors.Engineers != null)
                this.SongDetails.Contributors.Engineers.AddRange(contributors.Engineers);
            if (contributors.Lyricists != null)
                this.SongDetails.Contributors.Lyricists.AddRange(contributors.Lyricists);
            if (contributors.Producers != null)
                this.SongDetails.Contributors.Producers.AddRange(contributors.Producers);
            if (contributors.Songwriters != null)
                this.SongDetails.Contributors.Songwriters.AddRange(contributors.Songwriters);
            if (contributors.FeaturedArtists != null)
                this.SongDetails.Contributors.FeaturedArtists.AddRange(contributors.FeaturedArtists);
            if (contributors.NonFeaturedMusicians != null)
                this.SongDetails.Contributors.NonFeaturedMusicians.AddRange(contributors.NonFeaturedMusicians);
            if (contributors.NonFeaturedVocalists != null)
                this.SongDetails.Contributors.NonFeaturedVocalists.AddRange(contributors.NonFeaturedVocalists);
        }

        public void UpdateTrackingAccountIfApplies(string trackingAccountId, string profileId)
        {
            this.SongDetails.Contributors.Owners.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.Composers.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.Engineers.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.Lyricists.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.Producers.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.Songwriters.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.FeaturedArtists.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.NonFeaturedMusicians.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
            this.SongDetails.Contributors.NonFeaturedVocalists.ForEach(x => x.UpdateIfApplies(trackingAccountId, profileId));
        }
    }

    public enum SubType
    {
        Golden,
        Diamond
    }

    public class SongDetails
    {
        public SongDetails(
            string name,
            string artistName,
            string albumName,
            string description,
            string genre,
            decimal miningByStreaming,
            decimal miningByCuration)
        {
            this.Name = name;
            this.ArtistName = artistName;
            this.AlbumName = albumName;
            this.Description = description;
            this.Genre = genre;
            this.MiningByStreaming = miningByStreaming;
            this.MiningByCuration = miningByCuration;
            this.Contributors = new Contributors();
        }

        public string Name { get; }
        public string ArtistName { get; }
        public string AlbumName { get; }
        public string Description { get; }
        public string Genre { get; }
        public decimal MiningByStreaming { get; set; }
        public decimal MiningByCuration { get; set; }
        public Contributors Contributors { set; get; }
    }

    public class Contributors
    {
        public List<Contributor> Owners { set; get; } = new List<Contributor>();
        public List<Contributor> Songwriters { set; get; } = new List<Contributor>();
        public List<Contributor> Producers { set; get; } = new List<Contributor>();
        public List<Contributor> Engineers { set; get; } = new List<Contributor>();
        public List<Contributor> Composers { set; get; } = new List<Contributor>();
        public List<Contributor> Lyricists { set; get; } = new List<Contributor>();
        public List<Contributor> FeaturedArtists { set; get; } = new List<Contributor>();
        public List<Contributor> NonFeaturedMusicians { set; get; } = new List<Contributor>();
        public List<Contributor> NonFeaturedVocalists { set; get; } = new List<Contributor>();
    }

    public class Contributor
    {
        public Contributor(string profileId, string trackingAccountId, string email, decimal percentage)
        {
            this.ProfileId = profileId;
            this.TrackingAccountId = trackingAccountId;
            this.Percentage = percentage;
            this.Email = email;
        }

        public string ProfileId { private set; get; }
        public string TrackingAccountId { get; }
        public decimal Percentage { get; }
        public string Email { get; }

        public void UpdateIfApplies(string trackingAccountId, string profileId)
        {
            if (this.TrackingAccountId == trackingAccountId && string.IsNullOrWhiteSpace(this.ProfileId))
                this.ProfileId = profileId;
        }
    }
}