package it.costantino.astaapp.web.rest;

import static it.costantino.astaapp.domain.RosterAsserts.*;
import static it.costantino.astaapp.web.rest.TestUtil.createUpdateProxyForBean;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.costantino.astaapp.IntegrationTest;
import it.costantino.astaapp.domain.Roster;
import it.costantino.astaapp.repository.RosterRepository;
import it.costantino.astaapp.service.dto.RosterDTO;
import it.costantino.astaapp.service.mapper.RosterMapper;
import jakarta.persistence.EntityManager;
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
 * Integration tests for the {@link RosterResource} REST controller.
 */
@IntegrationTest
@AutoConfigureMockMvc
@WithMockUser
class RosterResourceIT {

    private static final Boolean DEFAULT_FULL = false;
    private static final Boolean UPDATED_FULL = true;

    private static final Long DEFAULT_PORT = 1L;
    private static final Long UPDATED_PORT = 2L;

    private static final Long DEFAULT_DIF = 1L;
    private static final Long UPDATED_DIF = 2L;

    private static final Long DEFAULT_CC = 1L;
    private static final Long UPDATED_CC = 2L;

    private static final Long DEFAULT_ATT = 1L;
    private static final Long UPDATED_ATT = 2L;

    private static final String ENTITY_API_URL = "/api/rosters";
    private static final String ENTITY_API_URL_ID = ENTITY_API_URL + "/{id}";

    private static Random random = new Random();
    private static AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    @Autowired
    private ObjectMapper om;

    @Autowired
    private RosterRepository rosterRepository;

    @Autowired
    private RosterMapper rosterMapper;

    @Autowired
    private EntityManager em;

    @Autowired
    private MockMvc restRosterMockMvc;

    private Roster roster;

    private Roster insertedRoster;

    /**
     * Create an entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Roster createEntity() {
        return new Roster().full(DEFAULT_FULL).port(DEFAULT_PORT).dif(DEFAULT_DIF).cc(DEFAULT_CC).att(DEFAULT_ATT);
    }

    /**
     * Create an updated entity for this test.
     *
     * This is a static method, as tests for other entities might also need it,
     * if they test an entity which requires the current entity.
     */
    public static Roster createUpdatedEntity() {
        return new Roster().full(UPDATED_FULL).port(UPDATED_PORT).dif(UPDATED_DIF).cc(UPDATED_CC).att(UPDATED_ATT);
    }

    @BeforeEach
    void initTest() {
        roster = createEntity();
    }

    @AfterEach
    void cleanup() {
        if (insertedRoster != null) {
            rosterRepository.delete(insertedRoster);
            insertedRoster = null;
        }
    }

    @Test
    @Transactional
    void createRoster() throws Exception {
        long databaseSizeBeforeCreate = getRepositoryCount();
        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);
        var returnedRosterDTO = om.readValue(
            restRosterMockMvc
                .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
                .andExpect(status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString(),
            RosterDTO.class
        );

        // Validate the Roster in the database
        assertIncrementedRepositoryCount(databaseSizeBeforeCreate);
        var returnedRoster = rosterMapper.toEntity(returnedRosterDTO);
        assertRosterUpdatableFieldsEquals(returnedRoster, getPersistedRoster(returnedRoster));

        insertedRoster = returnedRoster;
    }

    @Test
    @Transactional
    void createRosterWithExistingId() throws Exception {
        // Create the Roster with an existing ID
        roster.setId(1L);
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        long databaseSizeBeforeCreate = getRepositoryCount();

        // An entity with an existing ID cannot be created, so this API call must fail
        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeCreate);
    }

    @Test
    @Transactional
    void checkFullIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        roster.setFull(null);

        // Create the Roster, which fails.
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkPortIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        roster.setPort(null);

        // Create the Roster, which fails.
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkDifIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        roster.setDif(null);

        // Create the Roster, which fails.
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkCcIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        roster.setCc(null);

        // Create the Roster, which fails.
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void checkAttIsRequired() throws Exception {
        long databaseSizeBeforeTest = getRepositoryCount();
        // set the field null
        roster.setAtt(null);

        // Create the Roster, which fails.
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        restRosterMockMvc
            .perform(post(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isBadRequest());

        assertSameRepositoryCount(databaseSizeBeforeTest);
    }

    @Test
    @Transactional
    void getAllRosters() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        // Get all the rosterList
        restRosterMockMvc
            .perform(get(ENTITY_API_URL + "?sort=id,desc"))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.[*].id").value(hasItem(roster.getId().intValue())))
            .andExpect(jsonPath("$.[*].full").value(hasItem(DEFAULT_FULL)))
            .andExpect(jsonPath("$.[*].port").value(hasItem(DEFAULT_PORT.intValue())))
            .andExpect(jsonPath("$.[*].dif").value(hasItem(DEFAULT_DIF.intValue())))
            .andExpect(jsonPath("$.[*].cc").value(hasItem(DEFAULT_CC.intValue())))
            .andExpect(jsonPath("$.[*].att").value(hasItem(DEFAULT_ATT.intValue())));
    }

    @Test
    @Transactional
    void getRoster() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        // Get the roster
        restRosterMockMvc
            .perform(get(ENTITY_API_URL_ID, roster.getId()))
            .andExpect(status().isOk())
            .andExpect(content().contentType(MediaType.APPLICATION_JSON_VALUE))
            .andExpect(jsonPath("$.id").value(roster.getId().intValue()))
            .andExpect(jsonPath("$.full").value(DEFAULT_FULL))
            .andExpect(jsonPath("$.port").value(DEFAULT_PORT.intValue()))
            .andExpect(jsonPath("$.dif").value(DEFAULT_DIF.intValue()))
            .andExpect(jsonPath("$.cc").value(DEFAULT_CC.intValue()))
            .andExpect(jsonPath("$.att").value(DEFAULT_ATT.intValue()));
    }

    @Test
    @Transactional
    void getNonExistingRoster() throws Exception {
        // Get the roster
        restRosterMockMvc.perform(get(ENTITY_API_URL_ID, Long.MAX_VALUE)).andExpect(status().isNotFound());
    }

    @Test
    @Transactional
    void putExistingRoster() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the roster
        Roster updatedRoster = rosterRepository.findById(roster.getId()).orElseThrow();
        // Disconnect from session so that the updates on updatedRoster are not directly saved in db
        em.detach(updatedRoster);
        updatedRoster.full(UPDATED_FULL).port(UPDATED_PORT).dif(UPDATED_DIF).cc(UPDATED_CC).att(UPDATED_ATT);
        RosterDTO rosterDTO = rosterMapper.toDto(updatedRoster);

        restRosterMockMvc
            .perform(
                put(ENTITY_API_URL_ID, rosterDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isOk());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertPersistedRosterToMatchAllProperties(updatedRoster);
    }

    @Test
    @Transactional
    void putNonExistingRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(
                put(ENTITY_API_URL_ID, rosterDTO.getId())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithIdMismatchRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(
                put(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType(MediaType.APPLICATION_JSON)
                    .content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void putWithMissingIdPathParamRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(put(ENTITY_API_URL).with(csrf()).contentType(MediaType.APPLICATION_JSON).content(om.writeValueAsBytes(rosterDTO)))
            .andExpect(status().isMethodNotAllowed());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void partialUpdateRosterWithPatch() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the roster using partial update
        Roster partialUpdatedRoster = new Roster();
        partialUpdatedRoster.setId(roster.getId());

        partialUpdatedRoster.full(UPDATED_FULL).port(UPDATED_PORT).dif(UPDATED_DIF).att(UPDATED_ATT);

        restRosterMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedRoster.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedRoster))
            )
            .andExpect(status().isOk());

        // Validate the Roster in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertRosterUpdatableFieldsEquals(createUpdateProxyForBean(partialUpdatedRoster, roster), getPersistedRoster(roster));
    }

    @Test
    @Transactional
    void fullUpdateRosterWithPatch() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        long databaseSizeBeforeUpdate = getRepositoryCount();

        // Update the roster using partial update
        Roster partialUpdatedRoster = new Roster();
        partialUpdatedRoster.setId(roster.getId());

        partialUpdatedRoster.full(UPDATED_FULL).port(UPDATED_PORT).dif(UPDATED_DIF).cc(UPDATED_CC).att(UPDATED_ATT);

        restRosterMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, partialUpdatedRoster.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(partialUpdatedRoster))
            )
            .andExpect(status().isOk());

        // Validate the Roster in the database

        assertSameRepositoryCount(databaseSizeBeforeUpdate);
        assertRosterUpdatableFieldsEquals(partialUpdatedRoster, getPersistedRoster(partialUpdatedRoster));
    }

    @Test
    @Transactional
    void patchNonExistingRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If the entity doesn't have an ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, rosterDTO.getId())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithIdMismatchRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(
                patch(ENTITY_API_URL_ID, longCount.incrementAndGet())
                    .with(csrf())
                    .contentType("application/merge-patch+json")
                    .content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isBadRequest());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void patchWithMissingIdPathParamRoster() throws Exception {
        long databaseSizeBeforeUpdate = getRepositoryCount();
        roster.setId(longCount.incrementAndGet());

        // Create the Roster
        RosterDTO rosterDTO = rosterMapper.toDto(roster);

        // If url ID doesn't match entity ID, it will throw BadRequestAlertException
        restRosterMockMvc
            .perform(
                patch(ENTITY_API_URL).with(csrf()).contentType("application/merge-patch+json").content(om.writeValueAsBytes(rosterDTO))
            )
            .andExpect(status().isMethodNotAllowed());

        // Validate the Roster in the database
        assertSameRepositoryCount(databaseSizeBeforeUpdate);
    }

    @Test
    @Transactional
    void deleteRoster() throws Exception {
        // Initialize the database
        insertedRoster = rosterRepository.saveAndFlush(roster);

        long databaseSizeBeforeDelete = getRepositoryCount();

        // Delete the roster
        restRosterMockMvc
            .perform(delete(ENTITY_API_URL_ID, roster.getId()).with(csrf()).accept(MediaType.APPLICATION_JSON))
            .andExpect(status().isNoContent());

        // Validate the database contains one less item
        assertDecrementedRepositoryCount(databaseSizeBeforeDelete);
    }

    protected long getRepositoryCount() {
        return rosterRepository.count();
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

    protected Roster getPersistedRoster(Roster roster) {
        return rosterRepository.findById(roster.getId()).orElseThrow();
    }

    protected void assertPersistedRosterToMatchAllProperties(Roster expectedRoster) {
        assertRosterAllPropertiesEquals(expectedRoster, getPersistedRoster(expectedRoster));
    }

    protected void assertPersistedRosterToMatchUpdatableProperties(Roster expectedRoster) {
        assertRosterAllUpdatablePropertiesEquals(expectedRoster, getPersistedRoster(expectedRoster));
    }
}
