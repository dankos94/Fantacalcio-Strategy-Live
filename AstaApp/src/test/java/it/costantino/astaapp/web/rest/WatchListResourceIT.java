package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.WatchListAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.WatchList;
import it.costantino.astaapp.repository.WatchListRepository;
import it.costantino.astaapp.service.dto.WatchListDTO;
import it.costantino.astaapp.service.mapper.WatchListMapper;
import jakarta.persistence.EntityManager;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

/**
 * Integration tests for the {@link WatchListResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class WatchListResourceIT {

    private static final Instant DEFAULT_DATE = Instant.ofEpochMilli(0L);
    private static final Instant UPDATED_DATE = Instant.now().truncatedTo(ChronoUnit.MILLIS);

    private static final String DEFAULT_VERSION = "AAAAAAAAAA";
    private static final String UPDATED_VERSION = "BBBBBBBBBB";

    private static final String ENTITY_API_URL = "/api/watch-lists";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private WatchListRepository watchListRepository;

    @Autowired
    private WatchListMapper watchListMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restWatchListMockMvc;

    private WatchList watchList;

    private WatchList insertedWatchList;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static WatchList createEntity() {
        return new WatchList().date(DEFAULT_DATE).version(DEFAULT_VERSION);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static WatchList createUpdatedEntity() {
        return new WatchList().date(UPDATED_DATE).version(UPDATED_VERSION);
    }

    @BeforeEach
    void initTest() {
        watchList = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedWatchList != null) {
            watchListRepository.delete(insertedWatchList);
            insertedWatchList = null;
        }
    }

    @Test
    @Transactional
    void createWatchList() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);
        var returnedWatchListDTO = om.readValue(
            restWatchListMockMvc
                .perform(
                    post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(watchListDTO))
                )
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            WatchListDTO.class
        );

        // Validate the WatchList in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedWatchList = watchListMapper.toEntity(returnedWatchListDTO);
        assertWatchListUpdatableFieldsEquals(returnedWatchList, getPersistedWatchList(returnedWatchList));

        insertedWatchList = returnedWatchList;
    }

    @Test
    @Transactional
    void createWatchListWithExistingId() throws Exception {
        // Create the WatchList with an existing ID
        watchList.setId(1L);
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restWatchListMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(watchListDTO)))
            .andExpect(status().isBadRequest());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void getAllWatchLists() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        // Get all the watchListList
        restWatchListMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(watchList.getId().intValue())))
            .andExpect(jsonPath("$.[*].date").value(hasItem(DEFAULT_DATE.toString())))
            .andExpect(jsonPath("$.[*].version").value(hasItem(DEFAULT_VERSION)));
    }

    @Test
    @Transactional
    void getWatchList() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        // Get the watchList
        restWatchListMockMvc
            .perform(get(ENTITY_API_URL_ID, watchList.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(watchList.getId().intValue()))
            .andExpect(jsonPath("$.date").value(DEFAULT_DATE.toString()))
            .andExpect(jsonPath("$.version").value(DEFAULT_VERSION));
    }

    @Test
    @Transactional
    void getNonExistingWatchList() throws Exception {
        // Get the watchList
        restWatchListMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingWatchList() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the watchList
        WatchList updatedWatchList = watchListRepository.findById(watchList.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedWatchList are not directly saved in db
        em.detach(updatedWatchList);
        updatedWatchList.date(UPDATED_DATE).version(UPDATED_VERSION);
        WatchListDTO watchListDTO = watchListMapper.toDto(updatedWatchList);

        restWatchListMockMvc
            .perform(
                put(ENTITY_API_URL_ID, watchListDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isOk());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedWatchListToMatchAllProperties(updatedWatchList);
    }

    @Test
    @Transactional
    void putNonExistingWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(
                put(ENTITY_API_URL_ID, watchListDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(watchListDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateWatchListWithPatch() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the watchList using partial update
        WatchList partialUpdatedWatchList = new WatchList();
        partialUpdatedWatchList.setId(watchList.getId());

        restWatchListMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedWatchList.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedWatchList))
            )
            .andExpect(status().isOk());

        // Validate the WatchList in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertWatchListUpdatableFieldsEquals(
            createUpdateProxyForBean(partialUpdatedWatchList, watchList),
            getPersistedWatchList(watchList)
        );
    }

    @Test
    @Transactional
    void fullUpdateWatchListWithPatch() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the watchList using partial update
        WatchList partialUpdatedWatchList = new WatchList();
        partialUpdatedWatchList.setId(watchList.getId());

        partialUpdatedWatchList.date(UPDATED_DATE).version(UPDATED_VERSION);

        restWatchListMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedWatchList.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedWatchList))
            )
            .andExpect(status().isOk());

        // Validate the WatchList in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertWatchListUpdatableFieldsEquals(partialUpdatedWatchList, getPersistedWatchList(partialUpdatedWatchList));
    }

    @Test
    @Transactional
    void patchNonExistingWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, watchListDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamWatchList() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        watchList.setId(longCount.incrementAndGet());

        // Create the WatchList
        WatchListDTO watchListDTO = watchListMapper.toDto(watchList);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restWatchListMockMvc
            .perform(
                patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(watchListDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the WatchList in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteWatchList() throws Exception {
        // Initialize the database
        insertedWatchList = watchListRepository.saveAndFlush(watchList);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the watchList
        restWatchListMockMvc
            .perform(delete(ENTITY_API_URL_ID, watchList.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return watchListRepository.count();
    }

    protected void assertIncrementedRepositoryCount(long countBefore) {
        assertThat(countBefore + 1).isEqualTo(getRepositoryCount());
    }

    protected void assertDecrementedRepositoryCount(long countBefore) {
        assertThat(countBefore - 1).isEqualTo(getRepositoryCount());
    }

    protected void assertSameRepositoryCount(long countBefore) {
        assertThat(countBefore).isEqualTo(getRepositoryCount());
    }

    protected WatchList getPersistedWatchList(WatchList watchList) {
        return watchListRepository.findById(watchList.getId()).orElseThrow();
    }

    protected void assertPersistedWatchListToMatchAllProperties(WatchList expectedWatchList) {
        assertWatchListAllPropertiesEquals(expectedWatchList, getPersistedWatchList(expectedWatchList));
    }

    protected void assertPersistedWatchListToMatchUpdatableProperties(WatchList expectedWatchList) {
        assertWatchListAllUpdatablePropertiesEquals(expectedWatchList, getPersistedWatchList(expectedWatchList));
    }
}
